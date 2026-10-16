import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import { loadStudioSettings } from "@/lib/studio/settings-store";
import { sendAgentHandoffViaRuntime, sendDirectedAgentMessageViaRuntime } from "@/lib/runtime/agentMessaging";
import { parseAgentIdFromSessionKey } from "@/lib/gateway/sessionKeys";

type TeamAgent = { id: string; name?: string; role?: string; model?: string; status?: string; isDefault?: boolean };
type GatewayResult = { runId?: string; status?: string };
type BridgeSocket = {
  on(event: string, callback: (...args: unknown[]) => void): void;
  send(frame: string): void;
  close(): void;
};
type Frame = { type?: string; id?: string; ok?: boolean; payload?: unknown; error?: { message?: string } };
type WaitResult = { status?: string; sessionKey?: string; response?: string; error?: string };

export type TeamRunResult = {
  targetAgentId: string;
  runId: string;
  status: "completed" | "failed" | "timeout";
  response: string | null;
  elapsedMs: number;
  error?: string;
};
export type TeamGatewayClient = { request<T = unknown>(method: string, params: unknown): Promise<T>; close(): void };

const REQUEST_TIMEOUT_MS = 15_000;
const CONNECT_TIMEOUT_MS = 15_000;
const WAIT_POLL_MS = 5_000;
const DEFAULT_RUN_TIMEOUT_MS = 600_000;

// Next bundles by static analysis, so the CommonJS bridge is loaded from the
// deployed project root the same way `server/index.js` loads it.
const loadBridge = (): ((options: { url: string; token: string }) => BridgeSocket) => {
  const requireFromRoot = createRequire(`${process.cwd()}/package.json`);
  const bridge = requireFromRoot("./server/hermes-agent/bridge.js") as {
    createHermesAgentUpstream: (options: { url: string; token: string }) => BridgeSocket;
  };
  return bridge.createHermesAgentUpstream;
};

async function connectTeamGateway(): Promise<TeamGatewayClient> {
  const settings = loadStudioSettings();
  const gatewayUrl = settings.gateway?.url?.trim() || "";
  if (!gatewayUrl) throw new Error("Gateway URL is not configured.");
  if (settings.gateway?.adapterType !== "hermes-agent") {
    throw new Error("Team orchestration requires the hermes-agent gateway profile.");
  }
  const socket = loadBridge()({ url: gatewayUrl, token: settings.gateway?.token ?? "" });
  let nextId = 0;
  let closed = false;
  const pending = new Map<string, { resolve(value: unknown): void; reject(error: Error): void; timer: NodeJS.Timeout }>();
  const failAll = (error: Error) => {
    closed = true;
    for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(error); }
    pending.clear();
  };
  socket.on("message", (raw: unknown) => {
    let frame: Frame;
    try { frame = JSON.parse(String(raw)) as Frame; } catch { return; }
    if (frame.type !== "res" || !frame.id) return;
    const entry = pending.get(frame.id);
    if (!entry) return;
    pending.delete(frame.id);
    clearTimeout(entry.timer);
    if (frame.ok) entry.resolve(frame.payload);
    else entry.reject(new Error(frame.error?.message || "Gateway request failed."));
  });
  socket.on("error", (error: unknown) => failAll(error instanceof Error ? error : new Error(String(error))));
  socket.on("close", () => failAll(new Error("Hermes gateway connection closed.")));

  const client: TeamGatewayClient = {
    request<T>(method: string, params: unknown) {
      if (closed) return Promise.reject(new Error("Hermes gateway client is closed."));
      const id = `team-${++nextId}`;
      return new Promise<T>((resolve, reject) => {
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`Timed out waiting for the Hermes gateway to answer ${method}.`));
        }, REQUEST_TIMEOUT_MS);
        pending.set(id, { resolve: resolve as (value: unknown) => void, reject, timer });
        try { socket.send(JSON.stringify({ type: "req", id, method, params })); }
        catch (error) { pending.delete(id); clearTimeout(timer); reject(error instanceof Error ? error : new Error("Failed to reach the Hermes gateway.")); }
      });
    },
    close() {
      failAll(new Error("Hermes gateway client closed."));
      try { socket.close(); } catch { /* already closed */ }
    },
  };

  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Timed out connecting to the Hermes gateway.")), CONNECT_TIMEOUT_MS);
      socket.on("open", () => { clearTimeout(timer); resolve(); });
    });
    await client.request("connect", {});
    return client;
  } catch (error) {
    client.close();
    throw error;
  }
}

export async function withTeamGateway<T>(operation: (client: TeamGatewayClient) => Promise<T>): Promise<T> {
  const client = await connectTeamGateway();
  try { return await operation(client); } finally { client.close(); }
}

export async function listTeam(client: TeamGatewayClient): Promise<{ agents: TeamAgent[]; mainKey: string; defaultId: string }> {
  const data = await client.request<{ agents?: Array<Record<string, unknown>>; mainKey?: unknown; defaultId?: unknown }>("agents.list", {});
  const defaultId = typeof data.defaultId === "string" ? data.defaultId.trim() : "";
  const agents = (Array.isArray(data.agents) ? data.agents : []).flatMap((agent): TeamAgent[] => {
    const id = typeof agent.id === "string" ? agent.id.trim() : "";
    if (!id) return [];
    const optional = (key: string) => (typeof agent[key] === "string" ? (agent[key] as string) : undefined);
    return [{ id, name: optional("name"), role: optional("role"), model: optional("model"), status: optional("status"), isDefault: id === defaultId }];
  });
  return {
    agents,
    mainKey: typeof data.mainKey === "string" && data.mainKey.trim() ? data.mainKey.trim() : "main",
    defaultId,
  };
}

/**
 * Collect one run's exact result. A run is only trustworthy when the gateway
 * reports a terminal status together with the session the run belonged to;
 * anything else fails closed instead of reporting an empty success.
 */
async function collectRun(client: TeamGatewayClient, targetAgentId: string, runId: string, timeoutMs: number, startedAt: number): Promise<TeamRunResult> {
  const deadline = startedAt + timeoutMs;
  const finish = (partial: Omit<TeamRunResult, "elapsedMs">) => ({ ...partial, elapsedMs: Date.now() - startedAt });
  for (;;) {
    const remaining = deadline - Date.now();
    const wait: WaitResult = await client.request<WaitResult>("agent.wait", { runId, timeoutMs: Math.min(WAIT_POLL_MS, Math.max(1, remaining)) });
    if (wait?.status === "running") {
      if (Date.now() < deadline) continue;
      return finish({ targetAgentId, runId, status: "timeout", response: null, error: "Timed out waiting for the delegated run." });
    }
    if (wait?.status !== "completed" && wait?.status !== "error") {
      return finish({ targetAgentId, runId, status: "failed", response: null, error: "Gateway has no record of the delegated run." });
    }
    if (!wait.sessionKey) return finish({ targetAgentId, runId, status: "failed", response: null, error: "Gateway returned a run result without a session key." });
    if (parseAgentIdFromSessionKey(wait.sessionKey) !== targetAgentId) {
      return finish({ targetAgentId, runId, status: "failed", response: null, error: "Gateway returned a run result for a different agent session." });
    }
    if (wait.status === "error") return finish({ targetAgentId, runId, status: "failed", response: null, error: wait.error || "Delegated run failed." });
    const response = typeof wait.response === "string" ? wait.response : "";
    if (!response) return finish({ targetAgentId, runId, status: "failed", response: null, error: "Delegated run completed without an assistant response." });
    return finish({ targetAgentId, runId, status: "completed", response });
  }
}

export async function delegateToAgent(client: TeamGatewayClient, payload: {
  agentId: string; task: string; context?: string; deliverables?: string[]; acceptanceCriteria?: string; sourceAgentId?: string; timeoutMs?: number;
}): Promise<TeamRunResult> {
  const agentId = payload.agentId.trim();
  const roster = await listTeam(client);
  if (!roster.agents.some((agent) => agent.id === agentId)) throw new Error(`Unknown team agent: ${agentId || "(empty)"}`);
  if (agentId === roster.defaultId || payload.sourceAgentId?.trim() === agentId) throw new Error("Cannot delegate work to the current coordinator.");
  const sourceAgentId = payload.sourceAgentId?.trim() || roster.defaultId;
  const sent = await sendAgentHandoffViaRuntime({ call: client.request.bind(client) }, {
    targetAgentId: agentId, task: payload.task, context: payload.context,
    deliverables: payload.deliverables, acceptanceCriteria: payload.acceptanceCriteria,
    sourceAgentId, sourceLabel: roster.agents.find((agent) => agent.id === sourceAgentId)?.name, idempotencyKey: randomUUID(),
  }) as GatewayResult;
  const runId = typeof sent?.runId === "string" && sent.runId.trim() ? sent.runId.trim() : "";
  if (!runId) throw new Error(`Gateway did not return a run ID for ${agentId}.`);
  return collectRun(client, agentId, runId, payload.timeoutMs ?? DEFAULT_RUN_TIMEOUT_MS, Date.now());
}

export async function messageAgent(client: TeamGatewayClient, payload: { agentId: string; message: string; sourceAgentId?: string }) {
  const agentId = payload.agentId.trim();
  const roster = await listTeam(client);
  if (!roster.agents.some((agent) => agent.id === agentId)) throw new Error(`Unknown team agent: ${agentId || "(empty)"}`);
  if (agentId === roster.defaultId || payload.sourceAgentId?.trim() === agentId) throw new Error("Cannot message the current coordinator.");
  const startedAt = Date.now();
  const sourceAgentId = payload.sourceAgentId?.trim() || roster.defaultId;
  const sent = (await sendDirectedAgentMessageViaRuntime({ call: client.request.bind(client) }, {
    targetAgentId: agentId, message: payload.message,
    sourceAgentId, sourceLabel: roster.agents.find((agent) => agent.id === sourceAgentId)?.name, idempotencyKey: randomUUID(),
  })) as GatewayResult;
  const runId = typeof sent?.runId === "string" && sent.runId.trim() ? sent.runId.trim() : "";
  // Waiting before the caller closes the connection keeps the run alive: the
  // bridge drops the event stream and the session lock with the socket.
  if (!runId) return { targetAgentId: agentId, runId: null, status: "sent", elapsedMs: Date.now() - startedAt };
  return collectRun(client, agentId, runId, DEFAULT_RUN_TIMEOUT_MS, startedAt);
}

export async function delegateMany(client: TeamGatewayClient, payload: {
  assignments: Array<{ agentId: string; task: string; context?: string; deliverables?: string[]; acceptanceCriteria?: string; sourceAgentId?: string; timeoutMs?: number }>;
}): Promise<Array<TeamRunResult | { targetAgentId: string; status: "failed"; error: string }>> {
  return Promise.all(payload.assignments.map(async (assignment) => {
    try { return await delegateToAgent(client, assignment); }
    catch (error) { return { targetAgentId: assignment.agentId, status: "failed" as const, error: error instanceof Error ? error.message : "Delegation failed." }; }
  }));
}