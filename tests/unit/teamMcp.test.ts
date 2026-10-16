// @vitest-environment node
import { spawn } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

const TEAM_TOKEN = "team-token-mcp-test-0005";
const WRONG_TOKEN = "wrong-team-token-0006";
const MCP_SCRIPT = join(process.cwd(), "server", "hermes-team-mcp.mjs");

type McpReply = { id: number; result?: { isError?: boolean; content?: Array<{ text?: string }> } };
type FakeApi = { url: string; authorizations: string[] };

const servers: Server[] = [];
const children: Array<{ kill(): boolean }> = [];

afterEach(async () => {
  for (const child of children.splice(0)) child.kill();
  for (const server of servers.splice(0)) await new Promise<void>((resolve) => server.close(() => resolve()));
});

const startFakeTeamApi = async (acceptedToken: string): Promise<FakeApi> => {
  const authorizations: string[] = [];
  const server = createServer((req, res) => {
    const header = typeof req.headers.authorization === "string" ? req.headers.authorization : "";
    authorizations.push(header);
    if (header !== `Bearer ${acceptedToken}`) {
      res.writeHead(403, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "Team API token is invalid." }));
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ agents: [{ id: "coder", name: "Coder" }], mainKey: "main", defaultId: "default" }));
  });
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Fake team API did not bind to a port.");
  return { url: `http://127.0.0.1:${address.port}`, authorizations };
};

/** Run one roster tool call through the real MCP process; completes on its response line or exit. */
const callRosterTool = (env: Record<string, string | undefined>) =>
  new Promise<{ reply: McpReply; stdout: string; stderr: string }>((resolve, reject) => {
    const child = spawn(process.execPath, [MCP_SCRIPT], {
      env: { NODE_ENV: "test", PATH: process.env.PATH ?? "", ...env },
      stdio: "pipe",
    });
    children.push(child);
    let stdout = "";
    let stderr = "";
    let settled = false;
    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf8");
      const line = stdout.split("\n").find((entry) => entry.includes('"id":2'));
      if (!line || settled) return;
      settled = true;
      child.stdin.end();
      resolve({ reply: JSON.parse(line) as McpReply, stdout, stderr });
    });
    child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString("utf8"); });
    child.on("exit", (code) => {
      if (!settled) reject(new Error(`MCP exited (${code}) before answering. stderr=${stderr}`));
    });
    child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: "team_roster", arguments: {} } })}\n`);
  });

const writeTokenFile = (token: string) => {
  const dir = mkdtempSync(join(tmpdir(), "team-mcp-token-"));
  const file = join(dir, "team-token");
  writeFileSync(file, `${token}\n`, { mode: 0o600 });
  return file;
};

describe("Lucy team MCP authentication", () => {
  it("sends the team token as a bearer credential and does not print it", async () => {
    const api = await startFakeTeamApi(TEAM_TOKEN);

    const { reply, stdout, stderr } = await callRosterTool({
      HERMES_TEAM_API_URL: api.url,
      HERMES3D_TEAM_TOKEN_FILE: writeTokenFile(TEAM_TOKEN),
    });

    expect(reply.result?.isError).toBe(false);
    expect(reply.result?.content?.[0]?.text).toContain('"coder"');
    expect(api.authorizations).toEqual([`Bearer ${TEAM_TOKEN}`]);
    expect(stdout).not.toContain(TEAM_TOKEN);
    expect(stderr).not.toContain(TEAM_TOKEN);
  });

  it("makes no team API call when no team token is configured", async () => {
    const api = await startFakeTeamApi(TEAM_TOKEN);

    const { reply } = await callRosterTool({ HERMES_TEAM_API_URL: api.url });

    expect(reply.result?.isError).toBe(true);
    expect(reply.result?.content?.[0]?.text).toMatch(/not configured/);
    expect(api.authorizations).toEqual([]);
  });

  it("reports a rejected token as a tool error without echoing the presented token", async () => {
    const api = await startFakeTeamApi(TEAM_TOKEN);

    const { reply, stdout } = await callRosterTool({
      HERMES_TEAM_API_URL: api.url,
      HERMES3D_TEAM_TOKEN_FILE: writeTokenFile(WRONG_TOKEN),
    });

    expect(reply.result?.isError).toBe(true);
    expect(reply.result?.content?.[0]?.text).toContain("403");
    expect(api.authorizations).toEqual([`Bearer ${WRONG_TOKEN}`]);
    expect(stdout).not.toContain(WRONG_TOKEN);
  });
});
