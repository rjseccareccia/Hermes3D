import { describe, expect, it, vi } from "vitest";

import {
  delegateMany,
  delegateToAgent,
  listTeam,
  messageAgent,
  type TeamGatewayClient,
} from "@/lib/runtime/teamOrchestration";

type Handler = (method: string, params: unknown) => unknown;

const ROSTER = {
  defaultId: "lucy",
  mainKey: "main",
  agents: [
    { id: "lucy", name: "Lucy", role: "coordinator", model: "gpt", status: "idle", isDefault: true },
    { id: "coder", name: "Coder", role: "engineer", model: "claude", status: "idle" },
    { id: "researcher", name: "Researcher", role: "research", model: "gpt", status: "idle" },
  ],
};

const createClient = (handler: Handler) => {
  const calls: Array<{ method: string; params: unknown }> = [];
  const client: TeamGatewayClient = {
    request: async <T,>(method: string, params: unknown) => {
      calls.push({ method, params });
      return handler(method, params) as T;
    },
    close: () => {},
  };
  return { client, calls };
};

type WaitResult = (runId: string, sessionKey: string) => unknown;

const sessionKeyParam = (params: unknown): string => {
  if (params !== null && typeof params === "object" && "sessionKey" in params && typeof params.sessionKey === "string") {
    return params.sessionKey;
  }
  return "";
};

const runIdParam = (params: unknown): string => {
  if (params !== null && typeof params === "object" && "runId" in params && typeof params.runId === "string") {
    return params.runId;
  }
  return "";
};

/**
 * A gateway that records which session each `chat.send` targeted and answers
 * `agent.wait` for that run. Runs are correlated by run ID, never by order.
 */
const fakeGateway = (options: {
  sendFailure?: (sessionKey: string) => string | null;
  waitResult?: WaitResult;
} = {}) => {
  const runs = new Map<string, string>();
  let sequence = 0;
  const waitResult: WaitResult = options.waitResult
    ?? ((_runId, sessionKey) => ({ status: "completed", sessionKey, response: `done by ${sessionKey}` }));
  return createClient((method, params) => {
    if (method === "agents.list") return ROSTER;
    if (method === "chat.send") {
      const sessionKey = sessionKeyParam(params);
      const failure = options.sendFailure?.(sessionKey);
      if (failure) throw new Error(failure);
      const runId = `run-${++sequence}`;
      runs.set(runId, sessionKey);
      return { runId };
    }
    if (method === "agent.wait") {
      const runId = runIdParam(params);
      return waitResult(runId, runs.get(runId) ?? "");
    }
    throw new Error(`unexpected method ${method}`);
  });
};

describe("team orchestration", () => {
  it("lists the roster with identity, role, and model fields", async () => {
    const { client, calls } = createClient(() => ROSTER);

    const roster = await listTeam(client);

    expect(calls[0]).toEqual({ method: "agents.list", params: {} });
    expect(roster.defaultId).toBe("lucy");
    expect(roster.mainKey).toBe("main");
    expect(roster.agents.map((agent) => agent.id)).toEqual(["lucy", "coder", "researcher"]);
    expect(roster.agents[1]).toMatchObject({ id: "coder", role: "engineer", model: "claude", status: "idle" });
  });

  it("delegates a task and returns the exact completed run result", async () => {
    const { client, calls } = fakeGateway({
      waitResult: (_runId, sessionKey) => ({ status: "completed", sessionKey, response: "PR merged" }),
    });

    const result = await delegateToAgent(client, { agentId: "coder", task: "Ship the PR", sourceAgentId: "lucy" });

    expect(result).toMatchObject({ targetAgentId: "coder", runId: "run-1", status: "completed", response: "PR merged" });
    expect(result.elapsedMs).toBeGreaterThanOrEqual(0);
    expect(calls.map((call) => call.method)).toEqual(["agents.list", "agents.list", "chat.send", "agent.wait"]);
  });

  it("fails closed when a completed run carries no session key", async () => {
    const { client } = fakeGateway({
      waitResult: () => ({ status: "completed", response: "text without a session" }),
    });

    const result = await delegateToAgent(client, { agentId: "coder", task: "Ship it", sourceAgentId: "lucy" });

    expect(result).toMatchObject({ status: "failed", response: null });
    expect(result.error).toMatch(/session key/i);
  });

  it("rejects a run result that belongs to another agent's session", async () => {
    const { client } = fakeGateway({
      waitResult: () => ({ status: "completed", sessionKey: "agent:researcher:main", response: "research notes" }),
    });

    const result = await delegateToAgent(client, { agentId: "coder", task: "Ship it", sourceAgentId: "lucy" });

    expect(result).toMatchObject({ targetAgentId: "coder", status: "failed", response: null });
    expect(result.error).toMatch(/different agent session/i);
  });

  it("rejects an unknown target and the coordinator itself", async () => {
    const { client, calls } = createClient(() => ROSTER);

    await expect(delegateToAgent(client, { agentId: "ghost", task: "Nope", sourceAgentId: "lucy" })).rejects.toThrow(/Unknown team agent: ghost/);
    await expect(delegateToAgent(client, { agentId: "lucy", task: "Loop", sourceAgentId: "lucy" })).rejects.toThrow(/coordinator/i);
    expect(calls.every((call) => call.method === "agents.list")).toBe(true);
  });

  it("reports a run that stays running past the deadline as a timeout", async () => {
    const { client } = fakeGateway({ waitResult: () => ({ status: "running" }) });
    const now = vi.spyOn(Date, "now");
    let clock = 1_000;
    now.mockImplementation(() => (clock += 60));

    const result = await delegateToAgent(client, { agentId: "coder", task: "Long task", sourceAgentId: "lucy", timeoutMs: 120 });

    expect(result).toMatchObject({ status: "timeout", response: null, runId: "run-1" });
    now.mockRestore();
  });

  it("runs assignments in parallel with distinct run IDs, each correlated to its own profile", async () => {
    const { client } = fakeGateway();

    const results = await delegateMany(client, {
      assignments: [
        { agentId: "coder", task: "Code it", sourceAgentId: "lucy" },
        { agentId: "researcher", task: "Research it", sourceAgentId: "lucy" },
      ],
    });

    expect(results).toHaveLength(2);
    expect(results[0]).toMatchObject({ targetAgentId: "coder", status: "completed", response: "done by agent:coder:main" });
    expect(results[1]).toMatchObject({ targetAgentId: "researcher", status: "completed", response: "done by agent:researcher:main" });
    const runIds = results.map((result) => ("runId" in result ? result.runId : ""));
    expect(new Set(runIds).size).toBe(2);
  });

  it("isolates one failed target from the successful results of the others", async () => {
    const { client } = fakeGateway({
      sendFailure: (sessionKey) => (sessionKey.startsWith("agent:researcher:") ? "Target agent is no longer available." : null),
    });

    const results = await delegateMany(client, {
      assignments: [
        { agentId: "coder", task: "Code it", sourceAgentId: "lucy" },
        { agentId: "researcher", task: "Research it", sourceAgentId: "lucy" },
      ],
    });

    expect(results[0]).toMatchObject({ targetAgentId: "coder", status: "completed", response: "done by agent:coder:main" });
    expect(results[1]).toEqual({ targetAgentId: "researcher", status: "failed", error: "Target agent is no longer available." });
  });

  it("keeps a slow target as a timeout while the fast target still completes", async () => {
    const { client } = fakeGateway({
      waitResult: (_runId, sessionKey) => (sessionKey.startsWith("agent:researcher:")
        ? { status: "running" }
        : { status: "completed", sessionKey, response: "fast result" }),
    });
    const now = vi.spyOn(Date, "now");
    let clock = 1_000;
    now.mockImplementation(() => (clock += 60));

    const results = await delegateMany(client, {
      assignments: [
        { agentId: "coder", task: "Fast", sourceAgentId: "lucy" },
        { agentId: "researcher", task: "Slow", sourceAgentId: "lucy", timeoutMs: 120 },
      ],
    });

    expect(results[0]).toMatchObject({ targetAgentId: "coder", status: "completed", response: "fast result" });
    expect(results[1]).toMatchObject({ targetAgentId: "researcher", status: "timeout", response: null });
    now.mockRestore();
  });

  it("messages a teammate and waits for that run before closing", async () => {
    const waits: string[] = [];
    const { client } = fakeGateway({
      waitResult: (runId, sessionKey) => {
        waits.push(runId);
        return { status: "completed", sessionKey, response: "ack" };
      },
    });

    const result = await messageAgent(client, { agentId: "researcher", message: "status?", sourceAgentId: "lucy" });

    expect(waits).toEqual(["run-1"]);
    expect(result).toMatchObject({ targetAgentId: "researcher", status: "completed", response: "ack" });
  });
});
