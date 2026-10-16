// @vitest-environment node
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer, type WebSocket as WsSocket } from "ws";
import type { AddressInfo } from "node:net";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const { buildJsonRpcUrl, redactUrl } = await import("../../server/hermes-agent/jsonrpc-client");
const { createHermesAgentUpstream, toHermes3dMessages } = await import(
  "../../server/hermes-agent/bridge"
);

type Frame = Record<string, unknown>;
type RpcHandler = (params: Frame, emit: (type: string, payload: Frame) => void) => Frame | void;

/** Read a dotted path out of a decoded frame without widening everything to `any`. */
const at = (source: unknown, path: string): unknown =>
  path
    .split(".")
    .reduce<unknown>((acc, key) => (acc as Record<string, unknown> | undefined)?.[key], source);

const servers: WebSocketServer[] = [];
const upstreams: { terminate: () => void }[] = [];

afterEach(async () => {
  for (const upstream of upstreams.splice(0)) {
    try {
      upstream.terminate();
    } catch {}
  }
  for (const server of servers.splice(0)) {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

/**
 * Minimal stand-in for hermes-agent's /api/ws: emits gateway.ready on connect,
 * answers JSON-RPC requests from `handlers`, and lets a handler push events.
 */
const startFakeHermesAgent = async (handlers: Record<string, RpcHandler>) => {
  const wss = new WebSocketServer({ port: 0 });
  servers.push(wss);
  await new Promise<void>((resolve) => wss.on("listening", () => resolve()));

  const received: Frame[] = [];

  wss.on("connection", (ws: WsSocket, req) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    received.push({ __connect: true, path: url.pathname, token: url.searchParams.get("token") });

    const send = (obj: unknown) => ws.send(JSON.stringify(obj));
    const emit = (type: string, payload: Frame) =>
      send({ jsonrpc: "2.0", method: "event", params: { type, session_id: "s1", payload } });

    send({ jsonrpc: "2.0", method: "event", params: { type: "gateway.ready", payload: {} } });

    ws.on("message", (raw) => {
      const request = JSON.parse(String(raw)) as Frame;
      received.push(request);
      const method = String(request.method);
      const handler = handlers[method] || (method === "session.list" ? () => ({ sessions: [] }) : undefined);
      if (!handler) {
        send({
          jsonrpc: "2.0",
          id: request.id,
          error: { code: -32601, message: "unknown method" },
        });
        return;
      }
      try {
        const result = handler((request.params ?? {}) as Frame, emit);
        send({ jsonrpc: "2.0", id: request.id, result: result ?? {} });
      } catch (error) {
        send({ jsonrpc: "2.0", id: request.id, error: { code: -32000, message: error instanceof Error ? error.message : String(error) } });
      }
    });
  });

  const { port } = wss.address() as AddressInfo;
  return { url: `ws://127.0.0.1:${port}`, received };
};

/** Drive the bridge and collect the frames it sends back toward the browser. */
const openBridge = async (url: string, token = "") => {
  const frames: Frame[] = [];
  const sessionSelectionPath = join(mkdtempSync(join(tmpdir(), "hermes-bridge-test-")), "selection.json");
  const upstream = createHermesAgentUpstream({ url, token, sessionSelectionPath });
  upstreams.push(upstream);
  upstream.on("message", (raw: string) => frames.push(JSON.parse(raw) as Frame));

  await new Promise<void>((resolve, reject) => {
    upstream.on("open", () => resolve());
    upstream.on("error", reject);
    setTimeout(() => reject(new Error("bridge did not open")), 5000);
  });

  const send = (frame: Frame) => upstream.send(JSON.stringify(frame));

  const waitFor = async (predicate: (frame: Frame) => boolean, label: string) => {
    const start = Date.now();
    while (Date.now() - start < 5000) {
      const hit = frames.find(predicate);
      if (hit) return hit;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error(`timed out waiting for ${label}; saw ${JSON.stringify(frames)}`);
  };

  return { upstream, frames, send, waitFor };
};

const connectBridge = async (bridge: {
  send: (frame: Frame) => void;
  waitFor: (predicate: (frame: Frame) => boolean, label: string) => Promise<Frame>;
}) => {
  bridge.send({ type: "req", id: "test-connect", method: "connect", params: {} });
  await bridge.waitFor((frame) => frame.type === "res" && frame.id === "test-connect", "connect");
};

describe("buildJsonRpcUrl", () => {
  it("appends the gateway path and maps https to wss", () => {
    expect(buildJsonRpcUrl("https://host.ts.net:8443", "abc")).toBe(
      "wss://host.ts.net:8443/api/ws?token=abc",
    );
  });

  it("keeps a path the caller already supplied", () => {
    expect(buildJsonRpcUrl("wss://host.ts.net:8443/api/ws", "")).toBe(
      "wss://host.ts.net:8443/api/ws",
    );
  });

  it("maps http to ws and tolerates a trailing slash", () => {
    expect(buildJsonRpcUrl("http://localhost:9119/", "t")).toBe(
      "ws://localhost:9119/api/ws?token=t",
    );
  });

  it("rejects a scheme that is not http(s) or ws(s)", () => {
    expect(() => buildJsonRpcUrl("ftp://host", "")).toThrow(/Unsupported scheme/);
  });

  it("keeps the token out of logged URLs", () => {
    expect(redactUrl("wss://h/api/ws?token=secret")).toBe("wss://h/api/ws?token=***");
  });
});

describe("loopback Host fallback", () => {
  /**
   * Stands in for a loopback-bound hermes-agent behind Tailscale Serve, which
   * forwards the client's Host verbatim: the tailnet name is refused with 4403
   * and only a loopback Host gets through.
   */
  const startHostStrictAgent = async () => {
    const wss = new WebSocketServer({ port: 0 });
    servers.push(wss);
    await new Promise<void>((resolve) => wss.on("listening", () => resolve()));

    const hostsSeen: string[] = [];
    wss.on("connection", (ws: WsSocket, req) => {
      const host = String(req.headers.host ?? "");
      hostsSeen.push(host);
      const hostOnly = host.split(":")[0].toLowerCase();
      if (!["localhost", "127.0.0.1", "::1"].includes(hostOnly)) {
        ws.close(4403, "host_mismatch");
        return;
      }
      ws.send(JSON.stringify({ jsonrpc: "2.0", method: "event", params: { type: "gateway.ready", payload: {} } }));
    });

    const { port } = wss.address() as AddressInfo;
    return { port, hostsSeen };
  };

  it("retries with a loopback Host when the backend refuses the forwarded one", async () => {
    const { port, hostsSeen } = await startHostStrictAgent();
    const { HermesAgentJsonRpcClient } = await import("../../server/hermes-agent/jsonrpc-client");

    // 127.0.0.1 resolves, but the Host header carries a name the backend rejects.
    const client = new HermesAgentJsonRpcClient({ url: `ws://127.0.0.1:${port}`, token: "t" });
    client.hostHeader = "box.ts.net";
    client.loopbackHostFallback = true;

    const ready = new Promise<void>((resolve, reject) => {
      client.on("ready", () => resolve());
      client.on("close", (code: number) => reject(new Error(`closed ${code}`)));
      setTimeout(() => reject(new Error("never became ready")), 5000);
    });
    client.connect();
    await ready;

    expect(hostsSeen[0]).toBe("box.ts.net");
    expect(hostsSeen[1]).toBe("localhost");
    expect(client.usedLoopbackHost).toBe(true);
    client.terminate();
  });

  it("retries when the upgrade is refused with HTTP 403 before accepting", async () => {
    // How a loopback-bound hermes-agent actually refuses a foreign Host on
    // /api/ws: the handshake is rejected outright rather than accepted-then-closed.
    const hostsSeen: string[] = [];
    const wss = new WebSocketServer({
      port: 0,
      verifyClient: ({ req }, done) => {
        const host = String(req.headers.host ?? "");
        hostsSeen.push(host);
        done(host.split(":")[0].toLowerCase() === "localhost", 403);
      },
    });
    servers.push(wss);
    await new Promise<void>((resolve) => wss.on("listening", () => resolve()));
    wss.on("connection", (ws: WsSocket) => {
      ws.send(JSON.stringify({ jsonrpc: "2.0", method: "event", params: { type: "gateway.ready", payload: {} } }));
    });
    const { port } = wss.address() as AddressInfo;

    const { HermesAgentJsonRpcClient } = await import("../../server/hermes-agent/jsonrpc-client");
    const client = new HermesAgentJsonRpcClient({ url: `ws://127.0.0.1:${port}`, token: "t" });
    client.hostHeader = "box.ts.net";

    const ready = new Promise<void>((resolve, reject) => {
      client.on("ready", () => resolve());
      client.on("error", (e: Error) => reject(e));
      setTimeout(() => reject(new Error("never became ready")), 5000);
    });
    client.connect();
    await ready;

    expect(hostsSeen[0]).toBe("box.ts.net");
    expect(hostsSeen[1]).toBe("localhost");
    client.terminate();
  });

  it("gives up after one retry rather than looping", async () => {
    const wss = new WebSocketServer({ port: 0 });
    servers.push(wss);
    await new Promise<void>((resolve) => wss.on("listening", () => resolve()));
    let attempts = 0;
    wss.on("connection", (ws: WsSocket) => {
      attempts += 1;
      ws.close(4403, "host_mismatch");
    });
    const { port } = wss.address() as AddressInfo;

    const { HermesAgentJsonRpcClient } = await import("../../server/hermes-agent/jsonrpc-client");
    const client = new HermesAgentJsonRpcClient({ url: `ws://127.0.0.1:${port}`, token: "t" });

    const closed = new Promise<number>((resolve) => client.on("close", (code: number) => resolve(code)));
    client.connect();
    expect(await closed).toBe(4403);
    expect(attempts).toBe(2);
  });
});

describe("profiles as agents", () => {
  // Verbatim rows from a live hermes-agent `profiles.list`.
  const backendProfiles = [
    {
      name: "default",
      path: "/Users/example/.hermes",
      is_default: true,
      model: "claude-haiku-4-5-20251001",
      description: "",
      display_name: "",
    },
    {
      name: "allan",
      path: "/Users/example/.hermes/profiles/allan",
      is_default: false,
      model: "claude-haiku-4-5-20251001",
      description:
        "Allan — technical planner and business systems analyst for Example Co. Converts Jira tickets into plans.",
      display_name: "",
    },
    {
      name: "andrew",
      path: "/Users/example/.hermes/profiles/andrew",
      is_default: false,
      model: "claude-opus-4-8",
      description: "Andrew — senior full-stack software developer for Example Co.",
      display_name: "",
    },
  ];

  it("gives every profile its own agent", async () => {
    const { toHermes3dAgents } = await import("../../server/hermes-agent/bridge");
    const agents = toHermes3dAgents(backendProfiles);
    expect(agents.map((a) => a.id)).toEqual(["default", "allan", "andrew"]);
    expect(agents.map((a) => a.name)).toEqual(["Default", "Allan", "Andrew"]);
  });

  it("routes non-default agents by profile and leaves the default unnamed", async () => {
    const { toHermes3dAgents } = await import("../../server/hermes-agent/bridge");
    const [def, allan] = toHermes3dAgents(backendProfiles);
    // An empty profile means "launch profile" upstream; naming it is wrong.
    expect(def.profile).toBe("");
    expect(allan.profile).toBe("allan");
  });

  it("uses the description after the dash as the role", async () => {
    const { toHermes3dAgents } = await import("../../server/hermes-agent/bridge");
    const allan = toHermes3dAgents(backendProfiles)[1];
    expect(allan.role.startsWith("technical planner")).toBe(true);
  });

  it("picks the flagged profile as the default agent", async () => {
    const { toHermes3dAgents, resolveDefaultAgentId } = await import(
      "../../server/hermes-agent/bridge"
    );
    expect(resolveDefaultAgentId(toHermes3dAgents(backendProfiles))).toBe("default");
  });

  it("ignores unusable rows", async () => {
    const { toHermes3dAgents } = await import("../../server/hermes-agent/bridge");
    expect(toHermes3dAgents([null, {}, "x", { name: "" }])).toEqual([]);
    expect(toHermes3dAgents(undefined)).toEqual([]);
  });

  it("advertises every profile and creates sessions against the right one", async () => {
    const createCalls: Frame[] = [];
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: backendProfiles }),
      "session.create": (params) => {
        createCalls.push(params);
        return { session_id: `rt-${createCalls.length}` };
      },
      "prompt.submit": () => ({}),
    });
    const bridge = await openBridge(agent.url);

    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    bridge.send({ type: "req", id: "a1", method: "agents.list", params: {} });
    const listed = await bridge.waitFor((f) => f.type === "res" && f.id === "a1", "agents.list");
    expect((at(listed, "payload.agents") as unknown[]).length).toBe(3);

    // A prompt aimed at Allan's desk must run under Allan's profile.
    bridge.send({
      type: "req",
      id: "s1",
      method: "chat.send",
      params: { sessionKey: "agent:allan:main", message: "hi" },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "s1", "chat.send");
    expect(createCalls.at(-1)).toEqual({ profile: "allan" });

    // The default agent must NOT send a profile — that means "launch profile".
    bridge.send({
      type: "req",
      id: "s2",
      method: "chat.send",
      params: { sessionKey: "agent:default:main", message: "hi" },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "s2", "chat.send default");
    expect(createCalls.at(-1)).toEqual({});
  });

  it("falls back to a single agent when the backend has no profiles.list", async () => {
    const agent = await startFakeHermesAgent({});
    const bridge = await openBridge(agent.url);
    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");
    expect((at(res, "payload.snapshot.health.agents") as unknown[]).length).toBe(1);
    expect(at(res, "payload.snapshot.health.defaultAgentId")).toBe("hermes");
  });
});

describe("toHermes3dCronJobs", () => {
  // Verbatim row shape returned by a live hermes-agent `cron.manage` list.
  const agentJob = {
    job_id: "f43da87997a8",
    name: "Daily token spend - morning briefing",
    prompt_preview: "Run `hermes insights --days 1` and extract today's data.",
    schedule: "0 7 * * *",
    repeat: "forever",
    deliver: "origin",
    next_run_at: "2026-08-19T07:00:00-05:00",
    last_run_at: "2026-08-18T07:00:40.472539-05:00",
    last_status: "ok",
    last_delivery_error: null,
    last_fire_error: null,
    enabled: true,
    state: "scheduled",
  };

  it("fills in the fields the office task board reads unguarded", async () => {
    const { toHermes3dCronJobs } = await import("../../server/hermes-agent/bridge");
    const [job] = toHermes3dCronJobs([agentJob]);

    // These four are exactly what crashed the office page when forwarded raw.
    expect(job.id).toBe("f43da87997a8");
    expect(job.payload).toEqual({ kind: "agentTurn", message: agentJob.prompt_preview });
    expect(job.schedule).toEqual({ kind: "cron", expr: "0 7 * * *" });
    expect(typeof job.state).toBe("object");
    expect(job.state.lastStatus).toBe("ok");
    expect(job.state.nextRunAtMs).toBe(Date.parse(agentJob.next_run_at));
    expect(Number.isFinite(job.updatedAtMs)).toBe(true);
  });

  it("marks a running job so the board can show it as working", async () => {
    const { toHermes3dCronJobs } = await import("../../server/hermes-agent/bridge");
    const [job] = toHermes3dCronJobs([{ ...agentJob, state: "running" }]);
    expect(typeof job.state.runningAtMs).toBe("number");
  });

  it("surfaces a failure so the board can flag it", async () => {
    const { toHermes3dCronJobs } = await import("../../server/hermes-agent/bridge");
    const [job] = toHermes3dCronJobs([
      { ...agentJob, last_status: "error", last_fire_error: "boom" },
    ]);
    expect(job.state.lastStatus).toBe("error");
    expect(job.state.lastError).toBe("boom");
  });

  it("drops rows with no id and tolerates junk", async () => {
    const { toHermes3dCronJobs } = await import("../../server/hermes-agent/bridge");
    expect(toHermes3dCronJobs([{}, null, "nope", { name: "no id" }])).toEqual([]);
    expect(toHermes3dCronJobs(undefined)).toEqual([]);
  });

  it("reads the other schedule spellings hermes-agent emits", async () => {
    const { toHermes3dSchedule } = await import("../../server/hermes-agent/bridge");
    expect(toHermes3dSchedule("30m")).toEqual({ kind: "every", everyMs: 1_800_000 });
    expect(toHermes3dSchedule("every 2h")).toEqual({ kind: "every", everyMs: 7_200_000 });
    expect(toHermes3dSchedule("0 9 * * *")).toEqual({ kind: "cron", expr: "0 9 * * *" });
    expect(toHermes3dSchedule("2026-06-01T09:00:00Z")).toEqual({
      kind: "at",
      at: "2026-06-01T09:00:00Z",
    });
  });
});

describe("toHermes3dMessages", () => {
  it("renames text to content and drops non-conversational rows", () => {
    expect(
      toHermes3dMessages([
        { role: "user", text: "hi" },
        { role: "tool", name: "terminal" },
        { role: "assistant", text: "hello" },
      ]),
    ).toEqual([
      { role: "user", content: "hi" },
      { role: "assistant", content: "hello" },
    ]);
  });

  it("returns an empty list for a missing transcript", () => {
    expect(toHermes3dMessages(undefined)).toEqual([]);
  });
});

describe("hermes-agent bridge", () => {
  it("sends the token as a query param on /api/ws", async () => {
    const agent = await startFakeHermesAgent({});
    await openBridge(agent.url, "tok-123");

    expect(agent.received.find((frame) => frame.__connect)).toMatchObject({
      path: "/api/ws",
      token: "tok-123",
    });
  });

  it("answers connect with a hello-ok advertising one agent", async () => {
    const agent = await startFakeHermesAgent({});
    const bridge = await openBridge(agent.url);

    bridge.send({ type: "req", id: "c1", method: "connect", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "c1", "hello-ok");

    expect(res.ok).toBe(true);
    expect(at(res, "payload.type")).toBe("hello-ok");
    expect(at(res, "payload.snapshot.health.agents")).toHaveLength(1);
    expect(at(res, "payload.snapshot.health.defaultAgentId")).toBe("hermes");
  });

  it("turns chat.send into prompt.submit and streams deltas into chat events", async () => {
    const agent = await startFakeHermesAgent({
      "session.create": () => ({ session_id: "s1", stored_session_id: "stored-1" }),
      "prompt.submit": (_params, emit) => {
        setTimeout(() => {
          emit("message.start", {});
          emit("message.delta", { text: "Hel" });
          emit("message.delta", { text: "lo" });
          emit("message.complete", { text: "Hello", status: "complete" });
        }, 10);
        return { status: "streaming" };
      },
    });
    const bridge = await openBridge(agent.url);

    bridge.send({
      type: "req",
      id: "m1",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "hi", idempotencyKey: "run-1" },
    });

    const started = await bridge.waitFor((f) => f.type === "res" && f.id === "m1", "chat.send res");
    expect(started.payload).toMatchObject({ status: "started", runId: "run-1" });

    const submitted = agent.received.find((frame) => frame.method === "prompt.submit");
    expect(submitted?.params).toMatchObject({ session_id: "s1", text: "hi" });

    const final = await bridge.waitFor(
      (f) => f.event === "chat" && at(f, "payload.state") === "final",
      "final chat event",
    );
    expect(at(final, "payload.message")).toEqual({ role: "assistant", content: "Hello" });
    expect(at(final, "payload.runId")).toBe("run-1");

    // Deltas accumulate, so the browser always receives the full text so far.
    const deltas = bridge.frames.filter(
      (f) => f.event === "chat" && at(f, "payload.state") === "delta",
    );
    expect(deltas.map((frame) => at(frame, "payload.message.content"))).toEqual(["Hel", "Hello"]);

    bridge.send({ type: "req", id: "w1", method: "agent.wait", params: { runId: "run-1" } });
    const waited = await bridge.waitFor((f) => f.type === "res" && f.id === "w1", "agent.wait");
    expect(waited.payload).toEqual({
      sessionKey: "agent:hermes:main",
      status: "completed",
      response: "Hello",
    });

    const otherClient = await openBridge(agent.url);
    otherClient.send({ type: "req", id: "w2", method: "agent.wait", params: { runId: "run-1" } });
    const persisted = await otherClient.waitFor((f) => f.type === "res" && f.id === "w2", "persisted agent.wait");
    expect(persisted.payload).toEqual(waited.payload);
  });

  it("streams a run launched on one adapter to every other adapter on the backend", async () => {
    const agent = await startFakeHermesAgent({
      "session.create": () => ({ session_id: "s1" }),
      "prompt.submit": (_params, emit) => {
        setImmediate(() => {
          emit("message.delta", { text: "Hi" });
          emit("message.complete", { text: "Hi", status: "complete" });
        });
        return { status: "streaming" };
      },
    });
    const launcher = await openBridge(agent.url);
    const observer = await openBridge(agent.url);

    launcher.send({
      type: "req",
      id: "o1",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "hi", idempotencyKey: "run-shared" },
    });
    await launcher.waitFor((f) => f.type === "res" && f.id === "o1", "chat.send res");

    const seen = await observer.waitFor(
      (f) => f.event === "chat" && at(f, "payload.state") === "final",
      "final chat event on the second adapter",
    );
    expect(at(seen, "payload.runId")).toBe("run-shared");
    expect(at(seen, "payload.sessionKey")).toBe("agent:hermes:main");
    expect(at(seen, "payload.message")).toEqual({ role: "assistant", content: "Hi" });
  });


  it("reports a failed prompt as an error response rather than a silent hang", async () => {
    const agent = await startFakeHermesAgent({
      "session.create": () => ({ session_id: "s1" }),
    });
    const bridge = await openBridge(agent.url);

    bridge.send({
      type: "req",
      id: "m2",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "hi" },
    });

    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "m2", "chat.send failure");
    expect(res.ok).toBe(false);
    expect(at(res, "error.code")).toBe("hermes_agent.prompt_failed");
  });

  it("maps chat.abort onto session.interrupt", async () => {
    const agent = await startFakeHermesAgent({
      "session.create": () => ({ session_id: "s1" }),
      "prompt.submit": () => ({ status: "streaming" }),
      "session.interrupt": () => ({ status: "interrupted" }),
    });
    const bridge = await openBridge(agent.url);

    bridge.send({
      type: "req",
      id: "m3",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "hi", idempotencyKey: "run-9" },
    });
    await bridge.waitFor((f) => f.type === "res" && f.id === "m3", "chat.send res");
    bridge.send({
      type: "req",
      id: "m3-overlap",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "overlap" },
    });
    const overlap = await bridge.waitFor((f) => f.type === "res" && f.id === "m3-overlap", "overlapping send");
    expect(overlap.ok).toBe(false);
    expect(at(overlap, "error.code")).toBe("hermes_agent.run_in_progress");


    bridge.send({ type: "req", id: "a1", method: "chat.abort", params: { runId: "run-9" } });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "a1", "abort res");

    expect(res.payload).toMatchObject({ ok: true, aborted: 1 });
    expect(agent.received.some((frame) => frame.method === "session.interrupt")).toBe(true);
  });

  it("releases the session when the adapter that launched its run closes", async () => {
    const agent = await startFakeHermesAgent({
      "session.create": () => ({ session_id: "s1" }),
      "prompt.submit": () => ({ status: "streaming" }),
    });
    const launcher = await openBridge(agent.url);
    launcher.send({
      type: "req",
      id: "orphan-send",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "hi", idempotencyKey: "run-orphan" },
    });
    await launcher.waitFor((f) => f.type === "res" && f.id === "orphan-send", "orphan send");
    launcher.upstream.close(1000, "adapter closed");

    const next = await openBridge(agent.url);
    next.send({
      type: "req",
      id: "next-send",
      method: "chat.send",
      params: { sessionKey: "agent:hermes:main", message: "again", idempotencyKey: "run-next" },
    });
    const sent = await next.waitFor((f) => f.type === "res" && f.id === "next-send", "next send");
    expect(sent.ok).toBe(true);

    next.send({ type: "req", id: "orphan-wait", method: "agent.wait", params: { runId: "run-orphan", timeoutMs: 1000 } });
    const wait = await next.waitFor((f) => f.type === "res" && f.id === "orphan-wait", "orphan wait");
    expect(wait.payload).toMatchObject({ status: "error", sessionKey: "agent:hermes:main" });
  });

  it("surfaces the selected stored hermes-agent session as the main alias", async () => {
    const agent = await startFakeHermesAgent({
      "session.list": () => ({
        sessions: [{ id: "20260409_abc", title: "Yesterday's chat", started_at: 1000 }],
      }),
    });
    const bridge = await openBridge(agent.url);

    bridge.send({ type: "req", id: "s1", method: "sessions.list", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "s1", "sessions.list");

    const sessions = at(res, "payload.sessions") as Frame[];
    expect(sessions.map((session) => session.key)).toEqual(["agent:hermes:main"]);
    expect(sessions[0]).toMatchObject({ storedSessionId: "20260409_abc", displayName: "Main · Yesterday's chat" });
  });

  it("keeps working when an optional upstream method is unavailable", async () => {
    const agent = await startFakeHermesAgent({});
    const bridge = await openBridge(agent.url);

    bridge.send({ type: "req", id: "k1", method: "models.list", params: {} });
    const res = await bridge.waitFor((f) => f.type === "res" && f.id === "k1", "models.list");

    expect(res.ok).toBe(false);
    expect(at(res, "error.code")).toBe("hermes_agent.models_unavailable");
  });

  it("builds a profile-specific canonical model union with membership", async () => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [
        { name: "default", is_default: true, model: "m1", provider: "Ollama-Cloud" },
        { name: "coder", model: "m2", provider: "minimax" },
      ] }),
      "model.options": (params) => ({
        providers: [{ slug: params.profile ? "minimax" : "ollama-cloud", models: ["m1", "shared"], capabilities: { shared: { reasoning: true, context_window: 8192 } } }],
      }),
    });
    const bridge = await openBridge(agent.url);
    await connectBridge(bridge);
    bridge.send({ type: "req", id: "catalog", method: "models.list", params: {} });
    const response = await bridge.waitFor((f) => f.type === "res" && f.id === "catalog", "catalog");
    expect(response.ok).toBe(true);
    expect(response.payload).toEqual({ models: expect.arrayContaining([
      { provider: "ollama-cloud", id: "m1", name: "m1", agentIds: ["default"] },
      { provider: "ollama-cloud", id: "shared", name: "shared", reasoning: true, contextWindow: 8192, agentIds: ["default"] },
      { provider: "minimax", id: "shared", name: "shared", reasoning: true, contextWindow: 8192, agentIds: ["coder"] },
    ]) });
  });

  it("reports model inventory RPC failures instead of fabricating fallback models", async () => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "default", is_default: true }] }),
      "model.options": () => { throw new Error("inventory offline"); },
    });
    const bridge = await openBridge(agent.url);
    await connectBridge(bridge);
    bridge.send({ type: "req", id: "catalog-error", method: "models.list", params: {} });
    const response = await bridge.waitFor((f) => f.type === "res" && f.id === "catalog-error", "catalog error");
    expect(response.ok).toBe(false);
    expect(at(response, "error.message")).toContain("inventory offline");
  });

  it("switches only the selected session through canonical session.mutate using the resumed fence", async () => {
    let currentModel = "base";
    let revision = 7;
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "default", is_default: true, model: "base", provider: "ollama-cloud" }] }),
      "session.list": () => ({ sessions: [
        { id: "main-stored", model: "base", provider: "ollama-cloud" },
        { id: "other", model: "unrelated", provider: "ollama-cloud" },
      ] }),
      "session.resume": () => ({ session_id: "runtime-1", session_key: "main-stored", revision, execution_generation: 3 }),
      "model.options": () => ({
        model: currentModel,
        provider: "ollama-cloud",
        providers: [{ slug: "ollama-cloud", models: ["base", "target"] }],
      }),
      "session.mutate": (params) => {
        expect(params).toMatchObject({
          session_id: "main-stored",
          operation: "model",
          payload: { model: "target", provider: "ollama-cloud" },
          expected_revision: 7,
          expected_generation: 3,
        });
        currentModel = "target";
        revision = 8;
        return { session_id: "runtime-1", operation: "model", revision: 8 };
      },
    });
    const bridge = await openBridge(agent.url);
    await connectBridge(bridge);
    bridge.send({ type: "req", id: "switch", method: "sessions.patch", params: { key: "agent:default:main", model: "ollama-cloud/target" } });
    const response = await bridge.waitFor((f) => f.type === "res" && f.id === "switch", "switch");
    expect(response.ok).toBe(true);
    expect(at(response, "payload.resolved")).toEqual({ model: "target", modelProvider: "ollama-cloud" });
    expect(agent.received.some((frame) => frame.method === "config.set")).toBe(false);
    const fenceReads = agent.received.filter((frame) => frame.method === "session.resume");
    expect(fenceReads.at(-1)).toMatchObject({ params: { session_id: "main-stored" } });

    bridge.send({ type: "req", id: "agents-after-switch", method: "agents.list", params: {} });
    const agents = await bridge.waitFor((f) => f.type === "res" && f.id === "agents-after-switch", "agents after switch");
    expect(at(agents, "payload.agents.0.model")).toBe("ollama-cloud/base");

    bridge.send({ type: "req", id: "sessions-after-switch", method: "sessions.list", params: {} });
    const sessions = await bridge.waitFor((f) => f.type === "res" && f.id === "sessions-after-switch", "sessions after switch");
    const sessionRows = at(sessions, "payload.sessions") as Frame[];
    expect(sessionRows[0]).toMatchObject({ model: "target", modelProvider: "ollama-cloud" });
    expect(sessionRows[1]).toMatchObject({ model: "unrelated", modelProvider: "ollama-cloud" });
  });

  it("unions multiple providers and models while deduplicating shared entries by profile", async () => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "default", is_default: true }, { name: "coder" }] }),
      "model.options": (params) => params.profile
        ? { providers: [{ slug: "openai", models: ["gpt-a", "gpt-b"] }, { slug: "minimax", models: ["shared"] }] }
        : { providers: [{ slug: "openai", models: ["gpt-a", "gpt-b"] }, { slug: "anthropic", models: ["claude"] }] },
    });
    const bridge = await openBridge(agent.url);
    await connectBridge(bridge);
    bridge.send({ type: "req", id: "union", method: "models.list", params: {} });
    const response = await bridge.waitFor((f) => f.type === "res" && f.id === "union", "model union");
    const models = at(response, "payload.models") as Frame[];
    expect(models).toEqual(expect.arrayContaining([
      { provider: "openai", id: "gpt-a", name: "gpt-a", agentIds: ["default", "coder"] },
      { provider: "openai", id: "gpt-b", name: "gpt-b", agentIds: ["default", "coder"] },
      { provider: "anthropic", id: "claude", name: "claude", agentIds: ["default"] },
      { provider: "minimax", id: "shared", name: "shared", agentIds: ["coder"] },
    ]));
  });

  it("returns an empty catalog when inventories contain no configured models", async () => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "default", is_default: true }] }),
      "model.options": () => ({ providers: [{ slug: "openai", models: [] }] }),
    });
    const bridge = await openBridge(agent.url);
    await connectBridge(bridge);
    bridge.send({ type: "req", id: "empty", method: "models.list", params: {} });
    const response = await bridge.waitFor((f) => f.type === "res" && f.id === "empty", "empty catalog");
    expect(response.payload).toEqual({ models: [] });
  });

  it("rejects a model switch when session.mutate fails", async () => {
    const { response } = await switchOutcome({
      "session.mutate": () => { throw new Error("setter refused"); },
    }, "openai/target");
    expect(response.ok).toBe(false);
    expect(at(response, "error.code")).toBe("hermes_agent.model_not_applied");
    expect(at(response, "error.message")).toContain("setter refused");
  });

  it("stops at confirmation_required and never confirms on the user's behalf", async () => {
    const { response, received } = await switchOutcome({
      "session.mutate": () => ({ status: "confirmation_required", confirm_message: "Confirm expensive model", confirm: "token-1" }),
    }, "openai/target");
    expect(response.ok).toBe(false);
    expect(at(response, "error.code")).toBe("hermes_agent.model_confirmation_required");
    expect(at(response, "error.message")).toContain("Confirm expensive model");
    expect(received.filter((frame) => frame.method === "session.mutate")).toHaveLength(1);
  });

  it.each(["revision_conflict", "stale_generation", "session_busy"])(
    "maps a %s refusal from session.mutate to its error code",
    async (reason) => {
      const { response } = await switchOutcome({
        "session.mutate": () => { throw new Error(reason); },
      }, "openai/target");
      expect(response.ok).toBe(false);
      expect(at(response, "error.code")).toBe(`hermes_agent.${reason}`);
    },
  );

  it("reports an explicit unsupported-backend error when the backend has no canonical session.mutate", async () => {
    const { response } = await switchOutcome({}, "openai/target");
    expect(response.ok).toBe(false);
    expect(at(response, "error.code")).toBe("hermes_agent.model_switch_unsupported");
    expect(at(response, "error.message")).toContain("session.mutate");
  });

  it("refuses to mutate when the backend does not report the session revision and generation", async () => {
    const { response, received } = await switchOutcome({
      "session.resume": () => ({ session_id: "runtime-1" }),
      "session.mutate": () => { throw new Error("must not be called without a fence"); },
    }, "openai/target");
    expect(response.ok).toBe(false);
    expect(at(response, "error.code")).toBe("hermes_agent.model_switch_unsupported");
    expect(received.some((frame) => frame.method === "session.mutate")).toBe(false);
  });

  it("does not report a deferred change as applied", async () => {
    const { response } = await switchOutcome({
      "session.mutate": () => ({ session_id: "runtime-1", operation: "model", status: "deferred", revision: 5 }),
    }, "openai/target");
    expect(response.ok).toBe(false);
    expect(at(response, "error.code")).toBe("hermes_agent.model_not_applied");
  });

  it("exposes profile model/provider in agents.list and session model/provider", async () => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "default", is_default: true, model: "m1", provider: "openai" }] }),
      "session.list": () => ({ sessions: [{ id: "stored", title: "Saved", model: "m2", provider: "anthropic" }] }),
    });
    const bridge = await openBridge(agent.url);
    await connectBridge(bridge);
    bridge.send({ type: "req", id: "agents", method: "agents.list", params: {} });
    const agents = await bridge.waitFor((f) => f.type === "res" && f.id === "agents", "agents");
    expect(at(agents, "payload.agents.0.model")).toBe("openai/m1");
    bridge.send({ type: "req", id: "sessions-models", method: "sessions.list", params: {} });
    const sessions = await bridge.waitFor((f) => f.type === "res" && f.id === "sessions-models", "sessions");
    expect(at(sessions, "payload.sessions.0.model")).toBe("m2");
    expect(at(sessions, "payload.sessions.0.modelProvider")).toBe("anthropic");
    expect(`${at(sessions, "payload.sessions.0.modelProvider")}/${at(sessions, "payload.sessions.0.model")}`).toBe("anthropic/m2");
  });
  it("rejects unknown provider/model selections without calling session.mutate", async () => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "default", is_default: true }] }),
      "session.create": () => ({ session_id: "runtime-1" }),
      "model.options": () => ({ providers: [{ slug: "openai", models: ["known"] }] }),
      "session.mutate": () => { throw new Error("must not be called"); },
    });
    const bridge = await openBridge(agent.url);
    await connectBridge(bridge);
    bridge.send({ type: "req", id: "invalid", method: "sessions.patch", params: { key: "agent:default:main", model: "other/missing" } });
    const response = await bridge.waitFor((f) => f.type === "res" && f.id === "invalid", "invalid selection");
    expect(response.ok).toBe(false);
    expect(at(response, "error.code")).toBe("hermes_agent.invalid_model");
    expect(agent.received.some((frame) => frame.method === "session.mutate")).toBe(false);
  });

  const switchOutcome = async (handlers: Record<string, RpcHandler>, model: string) => {
    const agent = await startFakeHermesAgent({
      "profiles.list": () => ({ profiles: [{ name: "default", is_default: true }] }),
      "session.create": () => ({ session_id: "runtime-1" }),
      "session.resume": () => ({ session_id: "runtime-1", revision: 4, execution_generation: 1 }),
      "model.options": () => ({ model: "old", provider: "openai", providers: [{ slug: "openai", models: ["target"] }] }),
      ...handlers,
    });
    const bridge = await openBridge(agent.url);
    await connectBridge(bridge);
    bridge.send({ type: "req", id: "switch", method: "sessions.patch", params: { key: "agent:default:main", model } });
    const response = await bridge.waitFor((f) => f.type === "res" && f.id === "switch", "switch outcome");
    return { response, received: agent.received };
  };

  it("does not report a committed receipt as applied when the effective model did not change", async () => {
    const { response } = await switchOutcome({
      "session.mutate": () => ({ session_id: "runtime-1", operation: "model", revision: 5 }),
    }, "openai/target");
    expect(response.ok).toBe(false);
    expect(at(response, "error.code")).toBe("hermes_agent.model_not_applied");
  });
});
