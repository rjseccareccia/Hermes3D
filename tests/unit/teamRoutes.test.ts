// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const orchestration = vi.hoisted(() => ({
  listTeam: vi.fn(),
  delegateToAgent: vi.fn(),
  delegateMany: vi.fn(),
  messageAgent: vi.fn(),
  withTeamGateway: vi.fn(),
}));

vi.mock("@/lib/runtime/teamOrchestration", () => orchestration);

import { GET as getRoster } from "@/app/api/team/roster/route";
import { POST as postDelegate } from "@/app/api/team/delegate/route";
import { POST as postDelegateMany } from "@/app/api/team/delegate-many/route";
import { POST as postMessage } from "@/app/api/team/message/route";

const TEAM_TOKEN = "team-token-route-test-0003";
const GATEWAY_TOKEN = "gateway-token-route-test-0004";
const ORIGINAL_ENV = { ...process.env };

const get = (authorization?: string) =>
  new Request("http://studio.test/api/team/roster", { headers: authorization === undefined ? {} : { authorization } });

const post = (path: string, body: unknown, authorization?: string) =>
  new Request(`http://studio.test${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...(authorization === undefined ? {} : { authorization }) },
    body: JSON.stringify(body),
  });

beforeEach(() => {
  vi.clearAllMocks();
  process.env.HERMES3D_TEAM_TOKEN = TEAM_TOKEN;
  process.env.HERMES3D_GATEWAY_TOKEN = GATEWAY_TOKEN;
  orchestration.withTeamGateway.mockImplementation(async (operation: (client: unknown) => unknown) =>
    operation({ request: async () => ({}), close: () => {} }),
  );
});

afterEach(() => {
  process.env = { ...ORIGINAL_ENV };
});

describe("team routes", () => {
  it("rejects roster reads without a token before any gateway work", async () => {
    const response = await getRoster(get());

    expect(response.status).toBe(401);
    expect(orchestration.withTeamGateway).not.toHaveBeenCalled();
  });

  it("rejects delegation with a wrong token and never dispatches", async () => {
    const response = await postDelegate(post("/api/team/delegate", { targetAgentId: "coder", task: "Ship it" }, "Bearer wrong-token"));

    expect(response.status).toBe(403);
    expect(orchestration.withTeamGateway).not.toHaveBeenCalled();
    expect(orchestration.delegateToAgent).not.toHaveBeenCalled();
  });

  it("does not accept the Hermes gateway token as a team credential", async () => {
    const response = await postMessage(
      post("/api/team/message", { targetAgentId: "coder", message: "status?" }, `Bearer ${GATEWAY_TOKEN}`),
    );

    expect(response.status).toBe(403);
    expect(orchestration.messageAgent).not.toHaveBeenCalled();
  });

  it("dispatches a parallel fan-out with the team token and returns one result per assignment", async () => {
    orchestration.delegateMany.mockResolvedValue([
      { targetAgentId: "coder", runId: "run-a", status: "completed", response: "ROUNDTRIP_B", elapsedMs: 5 },
      { targetAgentId: "researcher", status: "failed", error: "Target agent is no longer available." },
    ]);

    const response = await postDelegateMany(
      post(
        "/api/team/delegate-many",
        { assignments: [{ targetAgentId: "coder", task: "Code" }, { targetAgentId: "researcher", task: "Research" }] },
        `Bearer ${TEAM_TOKEN}`,
      ),
    );
    const body = (await response.json()) as { results: Array<{ targetAgentId: string; status: string }> };

    expect(response.status).toBe(200);
    expect(body.results.map((result) => [result.targetAgentId, result.status])).toEqual([
      ["coder", "completed"],
      ["researcher", "failed"],
    ]);
    expect(orchestration.delegateMany).toHaveBeenCalledTimes(1);
  });

  it("never returns the team or gateway token in roster output", async () => {
    orchestration.listTeam.mockResolvedValue({ agents: [{ id: "coder", name: "Coder" }], mainKey: "main", defaultId: "default" });

    const response = await getRoster(get(`Bearer ${TEAM_TOKEN}`));
    const text = await response.text();

    expect(response.status).toBe(200);
    expect(JSON.parse(text)).toMatchObject({ agents: [{ id: "coder", name: "Coder" }] });
    expect(text).not.toContain(TEAM_TOKEN);
    expect(text).not.toContain(GATEWAY_TOKEN);
  });

  it("fails closed on the roster when the team token is not configured", async () => {
    delete process.env.HERMES3D_TEAM_TOKEN;

    const response = await getRoster(get(`Bearer ${TEAM_TOKEN}`));

    expect(response.status).toBe(503);
    expect(orchestration.withTeamGateway).not.toHaveBeenCalled();
  });
});
