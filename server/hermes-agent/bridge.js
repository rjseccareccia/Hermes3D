/**
 * Translates the Hermes3D gateway protocol into hermes-agent's JSON-RPC 2.0 API.
 *
 * hermes-agent has no server that speaks the Hermes3D protocol — that protocol
 * came from OpenClaw. Rather than run a separate adapter process, this module
 * presents a virtual upstream to `gateway-proxy.js`: it exposes the small slice
 * of the `ws` WebSocket surface the proxy actually uses (`readyState`, `send`,
 * `close`, `terminate`, and the open/message/close/error events), so the proxy's
 * connect handling and lifecycle stay untouched.
 *
 * Hermes3D talks in agents and session keys; hermes-agent profiles own durable
 * session stores. Main keys alias a pinned stored session per backend/profile,
 * while runtime sessions attach lazily for history, previews, and prompts.
 */

const { EventEmitter } = require("node:events");
const { randomUUID } = require("node:crypto");

const { HermesAgentJsonRpcClient, redactUrl } = require("./jsonrpc-client");
const { createOfficeSpeechSubscriber } = require("./office-speech");
const { createSessionSelectionStore, selectInitialSession } = require("./session-selection");
const {
  KANBAN_TASK_ID_PREFIX,
  toHermes3dKanbanTaskRecord,
  toHermes3dKanbanTasks,
  toKanbanPatchBody,
  kanbanRequest,
} = require("./kanban");

/** Mirrors the numeric WebSocket readyState constants the proxy compares against. */
const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 3;

const AGENT_ID = "hermes";
const AGENT_NAME = "Hermes";
const MAIN_KEY = "main";
const MAIN_SESSION_KEY = `agent:${AGENT_ID}:${MAIN_KEY}`;

/** hermes-agent's `session.create` / `session.resume` can be slow on a cold profile. */
const SESSION_RPC_TIMEOUT_MS = 60_000;
/** Run results and session locks outlive individual browser WebSocket adapters. */
const sharedRuns = new Map();
const activeSessionRuns = new Map();
const runEventListeners = new Map();
const MAX_COMPLETED_RUNS = 256;
const RUN_RESULT_TTL_MS = 10 * 60_000;

const retainRunResult = (key, value) => {
  sharedRuns.set(key, value);
  const now = Date.now();
  for (const [entryKey, result] of sharedRuns) {
    if (result.status !== "running" && now - (result.atMs ?? 0) > RUN_RESULT_TTL_MS) {
      sharedRuns.delete(entryKey);
    }
  }
  while (sharedRuns.size > MAX_COMPLETED_RUNS) {
    const completed = [...sharedRuns].find(([, result]) => result.status !== "running");
    if (!completed) break;
    sharedRuns.delete(completed[0]);
  }
};


/**
 * The slice of the `ws` WebSocket surface `gateway-proxy.js` relies on.
 *
 * @typedef {import("node:events").EventEmitter & {
 *   readyState: number,
 *   send: (raw: string) => void,
 *   close: (code?: number, reason?: string) => void,
 *   terminate: () => void,
 * }} HermesAgentUpstream
 */

const resOk = (id, payload) => ({ type: "res", id, ok: true, payload: payload ?? {} });
const resErr = (id, code, message) => ({ type: "res", id, ok: false, error: { code, message } });

const asString = (value, fallback = "") =>
  typeof value === "string" && value.trim() ? value.trim() : fallback;

const errorMessage = (err) => {
  if (!err) return "hermes-agent request failed";
  if (typeof err === "string") return err;
  return err.message || String(err);
};

/** hermes-agent history rows use `text`; Hermes3D expects `content`. */
const toHermes3dMessages = (messages) => {
  if (!Array.isArray(messages)) return [];
  return messages
    .filter((m) => m && (m.role === "user" || m.role === "assistant"))
    .map((m) => ({
      role: m.role,
      content: typeof m.text === "string" ? m.text : String(m.content ?? ""),
    }));
};

const parseTimestampMs = (value) => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? undefined : parsed;
};

const DURATION_UNIT_MS = { s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };

/**
 * hermes-agent stores a schedule as one string; Hermes3D wants a tagged union.
 *
 * The string is a 5-field cron expression, a duration such as "30m", or an ISO
 * timestamp for a one-shot job.
 */
const toHermes3dSchedule = (raw) => {
  const value = asString(raw);
  if (!value) return { kind: "cron", expr: "" };

  const duration = /^every\s+(\d+)\s*([smhd])$/i.exec(value) || /^(\d+)\s*([smhd])$/i.exec(value);
  if (duration) {
    const unit = DURATION_UNIT_MS[duration[2].toLowerCase()];
    if (unit) return { kind: "every", everyMs: Number(duration[1]) * unit };
  }

  if (value.split(/\s+/).length === 5) return { kind: "cron", expr: value };

  const at = parseTimestampMs(value);
  if (at !== undefined) return { kind: "at", at: value };

  return { kind: "cron", expr: value };
};

const CRON_STATUSES = new Set(["ok", "error", "skipped"]);

/**
 * Translate hermes-agent cron rows into Hermes3D's `CronJobSummary`.
 *
 * The two shapes disagree on nearly every field: hermes-agent uses `job_id`, a
 * schedule string, `prompt_preview`, ISO timestamps, and a `state` *string*,
 * while Hermes3D expects `id`, a schedule object, a `payload` object, epoch
 * milliseconds, and a `state` object. Forwarding the raw rows crashes the
 * office task board, which reads `job.payload.kind` unguarded.
 */
const toHermes3dCronJobs = (jobs, agentId = AGENT_ID) => {
  if (!Array.isArray(jobs)) return [];
  return jobs
    .filter((job) => job && typeof job === "object")
    .map((job) => {
      const nextRunAtMs = parseTimestampMs(job.next_run_at);
      const lastRunAtMs = parseTimestampMs(job.last_run_at);
      const lastStatus = asString(job.last_status).toLowerCase();
      const lastError = asString(job.last_fire_error) || asString(job.last_delivery_error);
      const message =
        asString(job.prompt_preview) || asString(job.name) || "Scheduled job";

      return {
        id: asString(job.job_id) || asString(job.id),
        name: asString(job.name) || asString(job.job_id) || "Scheduled job",
        agentId,
        description: asString(job.prompt_preview) || undefined,
        enabled: job.enabled !== false,
        updatedAtMs: lastRunAtMs ?? nextRunAtMs ?? Date.now(),
        schedule: toHermes3dSchedule(job.schedule),
        sessionTarget: "isolated",
        wakeMode: "next-heartbeat",
        payload: { kind: "agentTurn", message },
        state: {
          ...(nextRunAtMs !== undefined ? { nextRunAtMs } : {}),
          ...(lastRunAtMs !== undefined ? { lastRunAtMs } : {}),
          ...(asString(job.state).toLowerCase() === "running"
            ? { runningAtMs: Date.now() }
            : {}),
          ...(CRON_STATUSES.has(lastStatus) ? { lastStatus } : {}),
          ...(lastError ? { lastError } : {}),
        },
        delivery: { mode: asString(job.deliver) && job.deliver !== "none" ? "announce" : "none" },
      };
    })
    .filter((job) => job.id);
};

/** Used when the backend predates `profiles.list` or the call fails. */
const fallbackAgent = () => ({
  id: AGENT_ID,
  name: AGENT_NAME,
  workspace: "",
  identity: { name: AGENT_NAME, emoji: "🤖" },
  role: "",
  profile: "",
});

/** Title-case a profile directory name for display ("allan" -> "Allan"). */
const toDisplayName = (name) =>
  name ? name.charAt(0).toUpperCase() + name.slice(1) : "";

/**
 * Turn hermes-agent's profile list into Hermes3D agents.
 *
 * A profile is the backend's unit of identity — its own model, skills, memory
 * and sessions — which is exactly what Hermes3D calls an agent. Collapsing them
 * into one entry hides the operator's whole fleet, so each profile gets a desk.
 *
 * The `profile` field is what routes sessions back; the default profile carries
 * an empty string because omitting `profile` means "launch profile" upstream.
 */
const toHermes3dAgents = (profiles) => {
  if (!Array.isArray(profiles)) return [];
  const agents = profiles
    .filter((p) => p && typeof p === "object" && asString(p.name))
    .map((p) => {
      const name = asString(p.name);
      const isDefault = p.is_default === true;
      const display = asString(p.display_name) || toDisplayName(name);
      // The long profile descriptions read as a role ("Allan — technical
      // planner…"); keep the part after the dash so the desk label stays short.
      const description = asString(p.description);
      const role = description.includes("—")
        ? description.split("—").slice(1).join("—").trim()
        : description;
      return {
        id: name,
        name: display,
        workspace: asString(p.path),
        identity: { name: display, emoji: isDefault ? "🤖" : "🧑‍💻" },
        role,
        model: asString(p.model),
        provider: asString(p.provider),
        isDefault,
        profile: isDefault ? "" : name,
      };
    });
  return agents;
};

const resolveDefaultAgentId = (agents) => {
  const explicit = agents.find((a) => a.isDefault);
  return explicit?.id ?? agents[0]?.id ?? AGENT_ID;
};

const modelSwitchValue = (provider, model) =>
  `${model} --provider ${provider} --session`;

const canonicalModelProvider = (provider) => asString(provider).toLowerCase().replace(/[^a-z0-9_-]/g, "");

const getModelRows = (result) => {
  const rows = [];
  for (const entry of Array.isArray(result?.providers) ? result.providers : []) {
    const provider = canonicalModelProvider(entry?.slug);
    if (!provider || !Array.isArray(entry?.models)) continue;
    for (const raw of entry.models) {
      const id = asString(typeof raw === "string" ? raw : raw?.id);
      if (!id) continue;
      const capability = entry?.capabilities?.[id];
      rows.push({
        provider,
        id,
        name: typeof raw === "object" && raw ? asString(raw.name, id) : id,
        ...(capability?.reasoning !== undefined ? { reasoning: Boolean(capability.reasoning) } : {}),
        ...(typeof capability?.context_window === "number" ? { contextWindow: capability.context_window } : {}),
      });
    }
  }
  return rows;
};

const parseModelRef = (value, rows) => {
  const raw = asString(value);
  const match = rows.find((row) => raw === `${row.provider}/${row.id}`);
  return match ? { provider: match.provider, id: match.id } : null;
};

const resultingModel = (value, provider, rows) => {
  const ref = parseModelRef(value, rows);
  if (ref) return ref;
  const id = asString(value);
  return id ? { provider: canonicalModelProvider(provider), id } : null;
};

function createHermesAgentUpstream(options) {
  const {
    url,
    token,
    handshakeTimeoutMs,
    sessionSelectionPath,
    log = () => {},
    logError = () => {},
  } = options || {};

  const upstream = /** @type {HermesAgentUpstream} */ (new EventEmitter());
  upstream.readyState = CONNECTING;

  let client;
  try {
    client = new HermesAgentJsonRpcClient({ url, token, handshakeTimeoutMs, log });
  } catch (err) {
    // Defer so the caller can attach listeners before the failure lands.
    setImmediate(() => upstream.emit("error", err));
    upstream.readyState = CLOSED;
    upstream.send = () => {};
    upstream.close = () => {};
    upstream.terminate = () => {};
    return upstream;
  }

  /** sessionKey -> { runtimeId, storedId, title } */
  const sessions = new Map();
  const selections = createSessionSelectionStore(url, sessionSelectionPath);
  const sessionLoads = new Map();
  const storedLoads = new Map();
  /** runtime session id -> sessionKey */
  const sessionKeyByRuntimeId = new Map();
  /**
   * Hermes3D agents, one per hermes-agent profile.
   *
   * Profiles are the backend's fleet: each is a fully isolated instance with
   * its own model, skills, memory, and session store. Resolved once at connect
   * because the set only changes when the operator creates or deletes one.
   */
  let agentRoster = [fallbackAgent()];
  let defaultAgentId = AGENT_ID;
  /** Optional feed of turns driven from other clients; see ./office-speech.js. */
  let officeSpeech = null;
  /** runId -> { sessionKey, runtimeId, buffer, aborted, registryKey, lockKey } */
  const activeRuns = new Map();
  /** sessionKey -> runId, so session-scoped events find their run. */
  const runBySessionKey = new Map();

  let seq = 0;
  let closed = false;
  let runListeners = runEventListeners.get(url);
  if (!runListeners) {
    runListeners = new Set();
    runEventListeners.set(url, runListeners);
  }
  const receiveRunEvent = (event, payload) => {
    if (!closed) upstream.emit("message", JSON.stringify({ type: "event", event, seq: seq++, payload }));
  };
  runListeners.add(receiveRunEvent);

  const emitFrame = (frame) => {
    if (closed) return;
    upstream.emit("message", JSON.stringify(frame));
  };

  const emitEvent = (event, payload) => {
    emitFrame({ type: "event", event, seq: seq++, payload });
  };

  const emitRunEvent = (event, payload) => {
    for (const listener of runListeners) listener(event, payload);
  };

  const emitChat = (runId, sessionKey, state, extra) => {
    emitRunEvent("chat", { runId, sessionKey, state, ...extra });
  };

  // --- session mapping ------------------------------------------------------

  const rememberSession = (sessionKey, result) => {
    const runtimeId = asString(result?.session_id);
    if (!runtimeId) throw new Error("hermes-agent did not return a session id.");
    const entry = {
      runtimeId,
      storedId: asString(result?.stored_session_id) || asString(result?.session_key),
      lineageRootId: asString(result?.lineageRootId) || asString(result?.stored_session_id) || asString(result?.session_key),
      title: asString(result?.info?.title) || asString(result?.title),
      model: asString(result?.info?.model),
      provider: canonicalModelProvider(result?.info?.provider),
    };
    sessions.set(sessionKey, entry);
    sessionKeyByRuntimeId.set(runtimeId, sessionKey);
    return entry;
  };

  /**
   * Split `agent:<agentId>:<tail>` into the agent and the stored session id.
   */
  const parseSessionKey = (sessionKey) => {
    const parts = String(sessionKey ?? "").split(":");
    if (parts[0] !== "agent" || parts.length < 3) {
      return { agentId: defaultAgentId, tail: "" };
    }
    return { agentId: parts[1] || defaultAgentId, tail: parts.slice(2).join(":") };
  };

  /**
   * The `profile` value to send upstream for an agent.
   *
   * Empty means "the launch profile", which is what hermes-agent expects for
   * the default; naming it explicitly is unnecessary and, for a backend with no
   * profiles at all, would be rejected.
   */
  const profileForAgent = (agentId) => {
    const agent = agentRoster.find((a) => a.id === agentId);
    if (!agent) throw new Error(`Unknown Hermes profile: ${agentId}`);
    return asString(agent.profile);
  };

  /** Main session of whichever agent is default; the roster is known only at runtime. */
  const defaultMainKey = () => `agent:${defaultAgentId}:${MAIN_KEY}`;

  /**
   * Resolve the runtime session backing a Hermes3D session key, creating or
   * resuming one on hermes-agent the first time the key is used.
   *
   * The agent segment of the key selects the profile, so a prompt sent to
   * `agent:allan:main` runs with Allan's model, skills, and session store.
   */
  const storedSessions = (agentId) => {
    if (storedLoads.has(agentId)) return storedLoads.get(agentId);
    const load = (async () => {
      const profile = profileForAgent(agentId);
      let limit = 200;
      for (;;) {
        const result = await client.request("session.list", {
          limit, ...(profile ? { profile } : {}),
        });
        if (!Array.isArray(result?.sessions)) throw new Error("Hermes returned an invalid session list.");
        // session.list has no offset. Grow the window so a pinned older session
        // cannot be mistaken for a deleted record.
        if (result.sessions.length < limit) return result.sessions.filter((row) => asString(row?.id));
        limit *= 2;
      }
    })();
    storedLoads.set(agentId, load);
    load.finally(() => storedLoads.delete(agentId)).catch(() => {});
    return load;
  };

  const selectMain = async (agentId, stored) => {
    const key = `agent:${agentId}:${MAIN_KEY}`;
    const current = sessions.get(key);
    if (current) return current;
    const saved = selections.get(agentId);
    const pinned = stored.find((row) => row.id === saved?.storedId
      || row._lineage_root_id === saved?.storedId
      || (saved?.lineageRootId && row._lineage_root_id === saved.lineageRootId));
    // Hermes mints draft keys before writing a DB row. Keep an explicitly new,
    // unsubmitted main empty across restart rather than reverting to old history.
    if (!pinned && saved?.draft && saved.storedId) {
      const entry = { storedId: saved.storedId, draft: true, title: "" };
      sessions.set(key, entry);
      return entry;
    }
    // Rank sources only for an unpinned/deleted selection. Within each category,
    // preserve Hermes' last-activity order, not started_at order.
    const selected = pinned || selectInitialSession(stored);
    if (!selected) return null;
    const entry = {
      storedId: selected.id, title: asString(selected.title),
      lineageRootId: asString(selected._lineage_root_id) || selected.id,
      model: asString(selected.model), provider: canonicalModelProvider(selected.provider),
    };
    selections.set(agentId, { storedId: entry.storedId, lineageRootId: entry.lineageRootId });
    sessions.set(key, entry);
    return entry;
  };

  const ensureSession = (sessionKey, create = true) => {
    if (sessionLoads.has(sessionKey)) return sessionLoads.get(sessionKey);
    const load = (async () => {
      const { agentId, tail } = parseSessionKey(sessionKey);
      const profile = profileForAgent(agentId);
      const scope = profile ? { profile } : {};
      let entry = sessions.get(sessionKey);
      if (entry?.runtimeId) return entry;
      const stored = await storedSessions(agentId);
      if (tail === MAIN_KEY) {
        entry = await selectMain(agentId, stored);
      } else {
        const row = stored.find((candidate) => candidate.id === tail || candidate._lineage_root_id === tail);
        if (!row) throw new Error(`Stored session not found in profile "${agentId}": ${tail}`);
        entry = { storedId: row.id, title: asString(row.title) };
      }
      if (entry?.draft && !create) return null;
      if (entry?.storedId) {
        const resumed = await client.request("session.resume", {
          session_id: entry.storedId, lazy: !entry.draft, omit_messages: true, ...scope,
        }, SESSION_RPC_TIMEOUT_MS);
        const remembered = rememberSession(sessionKey, { title: entry.title, ...resumed, lineageRootId: entry.lineageRootId, stored_session_id: asString(resumed?.session_key, entry.storedId) });
        if (tail === MAIN_KEY) selections.set(agentId, { storedId: remembered.storedId, lineageRootId: remembered.lineageRootId, ...(entry.draft ? { draft: true } : {}) });
        return remembered;
      }
      if (!create) return null;
      const created = await client.request("session.create", scope, SESSION_RPC_TIMEOUT_MS);
      const remembered = rememberSession(sessionKey, created);
      if (tail === MAIN_KEY) selections.set(agentId, { storedId: remembered.storedId, draft: true });
      log(`[hermes-agent] session for "${sessionKey}" -> profile "${profile || "(default)"}"`);
      return remembered;
    })();
    sessionLoads.set(sessionKey, load);
    load.finally(() => sessionLoads.delete(sessionKey)).catch(() => {});
    return load;
  };

  // --- upstream event fan-out ----------------------------------------------

  client.on("event", (type, runtimeSessionId, payload) => {
    const sessionKey = sessionKeyByRuntimeId.get(runtimeSessionId);
    if (!sessionKey) return;
    const runId = runBySessionKey.get(sessionKey);
    const run = runId ? activeRuns.get(runId) : null;

    switch (type) {
      case "message.start":
        if (run) run.buffer = "";
        return;

      case "message.delta": {
        if (!run || run.aborted) return;
        const text = typeof payload?.text === "string" ? payload.text : "";
        if (!text) return;
        run.buffer += text;
        emitChat(runId, sessionKey, "delta", {
          message: { role: "assistant", content: run.buffer },
        });
        return;
      }

      case "message.complete": {
        if (!run) return;
        const finalText =
          typeof payload?.text === "string" && payload.text ? payload.text : run.buffer;
        const error = run.aborted ? "Run aborted" :
          payload?.status === "error" || payload?.error
            ? asString(payload?.error, "hermes-agent reported an error") : null;
        const result = {
          sessionKey,
          status: error ? "error" : "completed",
          ...(error ? { error } : { response: finalText }),
          atMs: Date.now(),
        };
        retainRunResult(run.registryKey, result);
        activeSessionRuns.delete(run.lockKey);
        if (run.aborted) emitChat(runId, sessionKey, "aborted", {});
        else if (error) emitChat(runId, sessionKey, "error", { errorMessage: error });
        else {
          emitChat(runId, sessionKey, "final", {
            stopReason: "end_turn",
            message: { role: "assistant", content: finalText },
          });
          emitRunEvent("presence", {
            sessions: {
              recent: [{ key: sessionKey, updatedAt: Date.now() }],
              byAgent: [{
                agentId: parseSessionKey(sessionKey).agentId,
                recent: [{ key: sessionKey, updatedAt: Date.now() }],
              }],
            },
          });
        }
        activeRuns.delete(runId);
        runBySessionKey.delete(sessionKey);
        return;
      }

      case "tool.start":
        if (!run) return;
        emitRunEvent("agent", {
          runId,
          sessionKey,
          stream: "tool",
          data: { phase: "start", name: asString(payload?.name), text: asString(payload?.context) },
        });
        return;

      case "tool.complete":
        if (!run) return;
        emitRunEvent("agent", {
          runId,
          sessionKey,
          stream: "tool",
          data: { phase: "complete", name: asString(payload?.name), text: asString(payload?.summary) },
        });
        return;

      case "reasoning.delta":
      case "thinking.delta":
        if (!run) return;
        emitRunEvent("agent", {
          runId,
          sessionKey,
          stream: "reasoning",
          data: { phase: "delta", text: typeof payload?.text === "string" ? payload.text : "" },
        });
        return;

      case "status.update":
        if (!run) return;
        emitRunEvent("agent", {
          runId,
          sessionKey,
          stream: "lifecycle",
          data: { phase: asString(payload?.kind, "status"), text: asString(payload?.text) },
        });
        return;

      case "approval.request":
        emitRunEvent("exec.approval.requested", {
          id: asString(payload?.request_id),
          request: { command: asString(payload?.command), cwd: asString(payload?.cwd) },
          createdAtMs: Date.now(),
          expiresAtMs: Date.now() + 120_000,
        });
        return;

      case "error":
        if (!run) return;
        {
          const error = asString(payload?.message, "hermes-agent reported an error");
          retainRunResult(run.registryKey, { sessionKey, status: "error", error, atMs: Date.now() });
          activeSessionRuns.delete(run.lockKey);
          emitChat(runId, sessionKey, "error", { errorMessage: error });
          activeRuns.delete(runId);
          runBySessionKey.delete(sessionKey);
        }
        return;

      default:
    }
  });

  // --- method dispatch ------------------------------------------------------

  /** Load the fleet once per connection; a backend without profiles keeps one agent. */
  const loadAgentRoster = async () => {
    try {
      const result = await client.request("profiles.list", {}, SESSION_RPC_TIMEOUT_MS);
      const mapped = toHermes3dAgents(result?.profiles);
      if (mapped.length > 0) {
        agentRoster = mapped;
        defaultAgentId = resolveDefaultAgentId(mapped);
        log(`[hermes-agent] ${mapped.length} profile(s) mapped to agents: ${mapped.map((a) => a.id).join(", ")}`);
        return;
      }
      log("[hermes-agent] profiles.list returned nothing; using a single agent");
    } catch (err) {
      log(`[hermes-agent] profiles.list unavailable (${errorMessage(err)}); using a single agent`);
    }
  };

  /**
   * Relay a turn published by the office bridge plugin.
   *
   * The plugin names the profile that spoke, and Hermes3D names each agent
   * after its profile, so the two line up directly. A turn from a profile this
   * connection does not know about is dropped rather than guessed at.
   */
  const handlePublishedTurn = (turn) => {
    const agent = agentRoster.find((a) => a.id === turn.profile);
    if (!agent) {
      log(`[office-speech] no agent for profile "${turn.profile}"; turn ignored`);
      return;
    }
    emitEvent("office.speech", {
      agentId: agent.id,
      name: agent.name,
      text: turn.text,
      atMs: turn.atMs,
      sessionId: turn.sessionId,
    });
  };

  const startOfficeSpeech = () => {
    if (officeSpeech) return;
    officeSpeech = createOfficeSpeechSubscriber({
      url,
      token,
      onTurn: handlePublishedTurn,
      log,
    });
  };

  const handleConnect = async (id) => {
    await loadAgentRoster();
    // Only worth subscribing once the roster exists to map turns onto.
    startOfficeSpeech();
    const agents = agentRoster.map((a) => ({
      agentId: a.id,
      name: a.name,
      isDefault: a.id === defaultAgentId,
    }));
    return resOk(id, {
      type: "hello-ok",
      protocol: 3,
      // The client trusts this over the configured type once connected, so
      // report what the backend actually is — hermes-agent capabilities
      // (native kanban, profile fleet) hang off this detection.
      adapterType: "hermes-agent",
      features: {
        methods: [
          "agents.list",
          "agents.files.get",
          "agents.files.set",
          "sessions.list",
          "sessions.preview",
          "sessions.patch",
          "sessions.reset",
          "chat.send",
          "chat.abort",
          "chat.history",
          "agent.wait",
          "status",
          "config.get",
          "config.set",
          "config.patch",
          "exec.approvals.get",
          "exec.approvals.set",
          "exec.approval.resolve",
          "wake",
          "skills.status",
          "models.list",
          "tasks.list",
          "tasks.update",
          "cron.list",
        ],
        events: ["chat", "agent", "presence", "heartbeat", "cron"],
      },
      snapshot: {
        health: { agents, defaultAgentId },
        sessionDefaults: { mainKey: MAIN_KEY },
      },
      auth: { role: "operator", scopes: ["operator.admin", "operator.approvals"] },
      policy: { tickIntervalMs: 30_000 },
    });
  };

  const handleMethod = async (method, params, id) => {
    const p = params || {};

    switch (method) {
      case "agents.list":
        return resOk(id, {
          defaultId: defaultAgentId,
          mainKey: MAIN_KEY,
          agents: agentRoster.map((agent) => ({
            id: agent.id, name: agent.name, workspace: agent.workspace,
            identity: agent.identity, role: agent.role,
            ...(agent.model ? { model: `${canonicalModelProvider(agent.provider || "")}/${agent.model}` } : {}),
          })),
        });

      case "agents.files.get":
        return resOk(id, { file: { missing: true } });

      case "agents.files.set":
        return resOk(id, {});

      case "config.get":
        return resOk(id, {
          config: { gateway: { reload: { mode: "hot" } } },
          hash: "hermes-agent",
          exists: true,
          path: "",
        });

      case "config.patch":
      case "config.set":
        return resOk(id, { hash: "hermes-agent" });

      case "sessions.list": {
        // Each profile keeps its own session store, so the stored rows have to
        // be read per agent; they're local SQLite reads, so fan out in parallel.
        const perAgent = await Promise.all(
          agentRoster.map(async (agent) => {
            const stored = await storedSessions(agent.id);
            const main = await selectMain(agent.id, stored);
            return [
              {
                key: `agent:${agent.id}:${MAIN_KEY}`,
                agentId: agent.id,
                updatedAt: null,
                displayName: main?.title ? `Main · ${main.title}` : "Main",
                storedSessionId: main?.storedId,
                origin: { label: agent.name, provider: canonicalModelProvider(agent.provider || "") },
                model: sessions.get(`agent:${agent.id}:${MAIN_KEY}`)?.model || agent.model || undefined,
                modelProvider: sessions.get(`agent:${agent.id}:${MAIN_KEY}`)?.provider || canonicalModelProvider(agent.provider || "") || undefined,
              },
              ...stored.filter((s) => s.id !== main?.storedId).map((s) => ({
                key: `agent:${agent.id}:${asString(s.id)}`,
                agentId: agent.id,
                updatedAt: typeof s.started_at === "number" ? s.started_at * 1000 : null,
                displayName: asString(s.title, "Session"),
                origin: { label: agent.name, provider: canonicalModelProvider(s.provider || agent.provider || "") },
                model: sessions.get(`agent:${agent.id}:${asString(s.id)}`)?.model || asString(s.model) || undefined,
                modelProvider: sessions.get(`agent:${agent.id}:${asString(s.id)}`)?.provider || canonicalModelProvider(s.provider || agent.provider || "") || undefined,
              })),
            ];
          })
        );
        return resOk(id, { sessions: perAgent.flat() });
      }

      case "sessions.preview": {
        const keys = Array.isArray(p.keys) ? p.keys : [];
        const limit = typeof p.limit === "number" ? p.limit : 8;
        const maxChars = typeof p.maxChars === "number" ? p.maxChars : 240;
        const previews = await Promise.all(
          keys.map(async (key) => {
            try {
              const entry = await ensureSession(key, false);
              if (!entry) return { key, status: "empty", items: [] };
              const history = await client.request("session.history", {
                session_id: entry.runtimeId,
                ...(profileForAgent(parseSessionKey(key).agentId) ? { profile: profileForAgent(parseSessionKey(key).agentId) } : {}),
              });
              const items = toHermes3dMessages(history?.messages)
                .slice(-limit)
                .map((m) => ({
                  role: m.role,
                  text: m.content.slice(0, maxChars),
                  timestamp: Date.now(),
                }));
              return { key, status: items.length ? "ok" : "empty", items };
            } catch (err) {
              return { key, status: "error", error: errorMessage(err), items: [] };
            }
          })
        );
        return resOk(id, { ts: Date.now(), previews });
      }

      case "sessions.patch": {
        const key = asString(p.key, defaultMainKey());
        const model = typeof p.model === "string" ? p.model.trim() : "";
        let resolved;
        if (model) {
          const { agentId } = parseSessionKey(key);
          const agent = agentRoster.find((candidate) => candidate.id === agentId);
          const profile = asString(agent?.profile);
          const entry = await ensureSession(key);
          const inventory = await client.request("model.options", { ...(profile ? { profile } : {}), session_id: entry.runtimeId });
          const rows = getModelRows(inventory);
          const choice = parseModelRef(model, rows);
          if (!choice) return resErr(id, "hermes_agent.invalid_model", `Unknown model: ${model}`);
          const result = await client.request("config.set", {
            key: "model", value: modelSwitchValue(choice.provider, choice.id),
            session_id: entry.runtimeId, ...(profile ? { profile } : {}), scope: "session",
          });
          if (result?.confirm_required || result?.deferred) {
            return resErr(id, "hermes_agent.model_not_applied", asString(result?.confirm_message || result?.warning, "Model switch was not applied."));
          }
          const actual = await client.request("model.options", { ...(profile ? { profile } : {}), session_id: entry.runtimeId });
          resolved = resultingModel(actual?.model, actual?.provider, rows);
          if (!resolved || resolved.id !== choice.id || resolved.provider !== choice.provider) {
            return resErr(id, "hermes_agent.model_not_applied", "Hermes did not activate the requested model.");
          }
          entry.model = resolved.id;
          entry.provider = resolved.provider;
        }
        return resOk(id, {
          ok: true, key, entry: { thinkingLevel: p.thinkingLevel },
          ...(resolved ? { resolved: { model: resolved.id, modelProvider: resolved.provider } } : {}),
        });
      }

      case "sessions.reset": {
        const key = asString(p.key, defaultMainKey());
        const { agentId } = parseSessionKey(key);
        const profile = profileForAgent(agentId);
        // Serialize with in-flight hydration. Never close a resumed conversation:
        // other Hermes clients may still be attached to it.
        if (sessionLoads.has(key)) await sessionLoads.get(key);
        const created = await client.request("session.create", profile ? { profile } : {}, SESSION_RPC_TIMEOUT_MS);
        const mainKey = `agent:${agentId}:${MAIN_KEY}`;
        const previous = sessions.get(mainKey);
        const entry = rememberSession(mainKey, created);
        selections.set(agentId, { storedId: entry.storedId, draft: true });
        if (previous?.runtimeId) sessionKeyByRuntimeId.delete(previous.runtimeId);
        return resOk(id, { ok: true, key: mainKey, storedSessionId: entry.storedId });
      }

      case "chat.send": {
        const sessionKey = asString(p.sessionKey, defaultMainKey());
        const text =
          typeof p.message === "string" ? p.message.trim() : String(p.message ?? "").trim();
        const runId = asString(p.idempotencyKey) || randomUUID();
        if (!text) return resOk(id, { status: "no-op", runId });
        const registryKey = `${url}\n${runId}`;
        const lockKey = `${url}\n${sessionKey}`;
        if (activeSessionRuns.has(lockKey)) {
          return resErr(id, "hermes_agent.run_in_progress", "A run is already active for this session.");
        }

        let entry;
        try {
          entry = await ensureSession(sessionKey);
        } catch (err) {
          return resErr(id, "hermes_agent.session_failed", errorMessage(err));
        }
        if (activeSessionRuns.has(lockKey)) {
          return resErr(id, "hermes_agent.run_in_progress", "A run is already active for this session.");
        }
        retainRunResult(registryKey, { sessionKey, status: "running" });
        const run = { sessionKey, runtimeId: entry.runtimeId, buffer: "", aborted: false, registryKey, lockKey };
        activeRuns.set(runId, run);
        activeSessionRuns.set(lockKey, runId);
        runBySessionKey.set(sessionKey, runId);
        try {
          await client.request("prompt.submit", { session_id: entry.runtimeId, text });
          const { agentId, tail } = parseSessionKey(sessionKey);
          if (tail === MAIN_KEY && entry.storedId) selections.set(agentId, { storedId: entry.storedId, lineageRootId: entry.lineageRootId });
        } catch (err) {
          activeRuns.delete(runId);
          runBySessionKey.delete(sessionKey);
          activeSessionRuns.delete(lockKey);
          sharedRuns.set(registryKey, { sessionKey, status: "error", error: errorMessage(err), atMs: Date.now() });
          return resErr(id, "hermes_agent.prompt_failed", errorMessage(err));
        }
        return resOk(id, { status: "started", runId });
      }

      case "chat.abort": {
        const runId = asString(p.runId);
        const sessionKey = asString(p.sessionKey);
        const targets = runId
          ? [runId]
          : [...activeRuns.entries()]
              .filter(([, run]) => run.sessionKey === sessionKey)
              .map(([rid]) => rid);

        let aborted = 0;
        for (const rid of targets) {
          const run = activeRuns.get(rid);
          if (!run) continue;
          run.aborted = true;
          aborted += 1;
          try {
            await client.request("session.interrupt", { session_id: run.runtimeId });
          } catch (err) {
            log(`[hermes-agent] interrupt failed: ${errorMessage(err)}`);
          }
        }
        return resOk(id, { ok: true, aborted });
      }

      case "chat.history": {
        const sessionKey = asString(p.sessionKey, defaultMainKey());
        try {
          const entry = await ensureSession(sessionKey, false);
          if (!entry) return resOk(id, { sessionKey, messages: [] });
          const history = await client.request("session.history", {
            session_id: entry.runtimeId,
            ...(profileForAgent(parseSessionKey(sessionKey).agentId) ? { profile: profileForAgent(parseSessionKey(sessionKey).agentId) } : {}),
          });
          return resOk(id, { sessionKey, messages: toHermes3dMessages(history?.messages) });
        } catch (err) {
          log(`[hermes-agent] session.history failed: ${errorMessage(err)}`);
          return resErr(id, "hermes_agent.history_failed", errorMessage(err));
        }
      }

      case "agent.wait": {
        const runId = asString(p.runId);
        const registryKey = `${url}\n${runId}`;
        const timeoutMs = typeof p.timeoutMs === "number" ? p.timeoutMs : 30_000;
        const start = Date.now();
        while (sharedRuns.get(registryKey)?.status === "running" && Date.now() - start < timeoutMs) {
          await new Promise((r) => setTimeout(r, 50));
        }
        const result = sharedRuns.get(registryKey);
        if (!result) return resOk(id, { status: "unknown" });
        const { atMs, ...reported } = result;
        return resOk(id, reported);
      }


      case "status": {
        const recent = [...sessions.keys()].map((key) => ({ key, updatedAt: Date.now() }));
        const byAgent = agentRoster.map((agent) => ({
          agentId: agent.id,
          recent: recent.filter((entry) => parseSessionKey(entry.key).agentId === agent.id),
        }));
        return resOk(id, { sessions: { recent, byAgent } });
      }

      case "wake": {
        const text = asString(p.text);
        if (!text) return resOk(id, { ok: true });
        try {
          const entry = await ensureSession(defaultMainKey());
          await client.request("prompt.submit", { session_id: entry.runtimeId, text });
        } catch (err) {
          log(`[hermes-agent] wake failed: ${errorMessage(err)}`);
        }
        return resOk(id, { ok: true });
      }

      case "models.list": {
        const inventories = await Promise.all(agentRoster.map(async (agent) => {
          const profile = asString(agent.profile);
          try {
            return { agent, result: await client.request("model.options", { ...(profile ? { profile } : {}) }) };
          } catch (err) {
            return { agent, error: err };
          }
        }));
        const union = new Map();
        const failures = [];
        for (const { agent, result, error } of inventories) {
          if (error) {
            failures.push(`${agent.id}: ${errorMessage(error)}`);
            continue;
          }
          const providers = Array.isArray(result?.providers) ? result.providers : [];
          for (const provider of providers) {
            for (const row of getModelRows({ providers: [provider] })) {
              const key = `${row.provider}/${row.id}`;
              const existing = union.get(key);
              if (existing) existing.agentIds.push(agent.id);
              else union.set(key, { ...row, agentIds: [agent.id] });
            }
          }
        }
        if (failures.length) return resErr(id, "hermes_agent.models_unavailable", failures.join("; "));
        return resOk(id, { models: [...union.values()] });
      }

      case "skills.status": {
        try {
          const result = await client.request("skills.manage", { action: "list" });
          const skills = Array.isArray(result?.skills) ? result.skills : [];
          return resOk(id, { skills });
        } catch {
          return resOk(id, { skills: [] });
        }
      }

      case "cron.list": {
        try {
          // cron.manage reads the launch profile's scheduler, so the jobs
          // belong to whichever agent that profile maps to.
          const result = await client.request("cron.manage", { action: "list" });
          return resOk(id, { jobs: toHermes3dCronJobs(result?.jobs, defaultAgentId) });
        } catch (err) {
          log(`[hermes-agent] cron.manage failed: ${errorMessage(err)}`);
          return resOk(id, { jobs: [] });
        }
      }

      case "exec.approvals.get":
        return resOk(id, {
          path: "",
          exists: true,
          hash: "hermes-agent",
          file: {
            version: 1,
            defaults: { security: "full", ask: "off", autoAllowSkills: true },
            agents: {},
          },
        });

      case "exec.approvals.set":
        return resOk(id, { hash: "hermes-agent" });

      case "exec.approval.resolve": {
        const requestId = asString(p.id);
        const decision = asString(p.decision, "deny");
        const runtimeId = [...sessions.values()][0]?.runtimeId;
        if (requestId && runtimeId) {
          try {
            await client.request("approval.respond", {
              session_id: runtimeId,
              request_id: requestId,
              choice: decision === "allow" ? "once" : "deny",
            });
          } catch (err) {
            log(`[hermes-agent] approval.respond failed: ${errorMessage(err)}`);
          }
        }
        return resOk(id, { ok: true });
      }

      // Kanban is built into hermes-agent — the board rides the same origin
      // and session token as the JSON-RPC gateway, so the office task board
      // reflects the real `hermes kanban` board with nothing to install.
      case "tasks.list": {
        try {
          const includeArchived = p.includeArchived === false ? "false" : "true";
          const board = await kanbanRequest({
            wsUrl: url,
            token,
            useLoopbackHost: client.usedLoopbackHost,
            method: "GET",
            path: `/board?include_archived=${includeArchived}`,
          });
          return resOk(id, { tasks: toHermes3dKanbanTasks(board) });
        } catch (err) {
          // A hidden/disabled kanban plugin or older backend is not an error
          // state for the office — the board just has no hermes tasks.
          log(`[hermes-agent] kanban board unavailable: ${errorMessage(err)}`);
          return resOk(id, { tasks: [] });
        }
      }

      case "tasks.update": {
        const rawId = asString(p.id);
        if (!rawId.startsWith(KANBAN_TASK_ID_PREFIX)) {
          return resErr(
            id,
            "hermes_agent.tasks_update_unsupported",
            "Only Hermes kanban tasks can be updated on this backend.",
          );
        }
        const taskId = rawId.slice(KANBAN_TASK_ID_PREFIX.length);
        try {
          const result = await kanbanRequest({
            wsUrl: url,
            token,
            useLoopbackHost: client.usedLoopbackHost,
            method: "PATCH",
            path: `/tasks/${encodeURIComponent(taskId)}`,
            body: toKanbanPatchBody(p),
          });
          const record = toHermes3dKanbanTaskRecord(result?.task);
          if (!record) {
            return resErr(
              id,
              "hermes_agent.tasks_update_failed",
              "hermes-agent did not return the updated task.",
            );
          }
          return resOk(id, record);
        } catch (err) {
          return resErr(id, "hermes_agent.tasks_update_failed", errorMessage(err));
        }
      }

      default:
        log(`[hermes-agent] unhandled method: ${method}`);
        return resOk(id, {});
    }
  };

  // --- virtual WebSocket surface -------------------------------------------

  upstream.send = (raw) => {
    let frame;
    try {
      frame = JSON.parse(String(raw ?? ""));
    } catch {
      return;
    }
    if (!frame || frame.type !== "req") return;

    const { id, method, params } = frame;
    const respond = (result) => emitFrame(result);

    if (method === "connect") {
      handleConnect(id).then(respond, (err) =>
        respond(resErr(id, "hermes_agent.connect_failed", errorMessage(err)))
      );
      return;
    }

    handleMethod(method, params, id).then(respond, (err) => {
      logError(`[hermes-agent] method "${method}" failed.`, err);
      respond(resErr(id, "hermes_agent.request_failed", errorMessage(err)));
    });
  };

  const stopOfficeSpeech = () => {
    officeSpeech?.close();
    officeSpeech = null;
  };

  const stopRunEventForwarding = () => {
    runListeners.delete(receiveRunEvent);
    if (runListeners.size === 0) runEventListeners.delete(url);
  };

  /**
   * A run's completion only reaches the adapter that submitted it. Once that
   * adapter closes nothing here can finish the run, so release its session lock
   * now instead of blocking the session until Hermes3D restarts.
   */
  const releaseOwnedRuns = (reason) => {
    for (const [runId, run] of activeRuns) {
      retainRunResult(run.registryKey, { sessionKey: run.sessionKey, status: "error", error: reason, atMs: Date.now() });
      if (activeSessionRuns.get(run.lockKey) === runId) activeSessionRuns.delete(run.lockKey);
      if (runBySessionKey.get(run.sessionKey) === runId) runBySessionKey.delete(run.sessionKey);
    }
    activeRuns.clear();
  };

  upstream.close = (code, reason) => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    stopRunEventForwarding();
    releaseOwnedRuns("Run connection closed before the run finished.");
    client.close(code, reason);
  };

  upstream.terminate = () => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    stopRunEventForwarding();
    releaseOwnedRuns("Run connection closed before the run finished.");
    client.terminate();
  };

  client.on("ready", () => {
    upstream.readyState = OPEN;
    log(`[hermes-agent] JSON-RPC gateway ready at ${redactUrl(client.url)}`);
    upstream.emit("open");
  });

  client.on("close", (code, reason) => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    stopRunEventForwarding();
    releaseOwnedRuns("Run connection closed before the run finished.");
    upstream.emit("close", code, Buffer.from(String(reason ?? "")));
  });

  client.on("error", (err) => {
    upstream.emit("error", err);
  });

  client.connect();

  return upstream;
}

module.exports = {
  createHermesAgentUpstream,
  toHermes3dMessages,
  toHermes3dCronJobs,
  toHermes3dSchedule,
  toHermes3dAgents,
  resolveDefaultAgentId,
  MAIN_SESSION_KEY,
  AGENT_ID,
};
