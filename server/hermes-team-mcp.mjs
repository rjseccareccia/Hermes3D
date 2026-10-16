#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { request as httpRequest } from "node:http";

const API_BASE_URL = (process.env.HERMES_TEAM_API_URL || "http://10.0.0.20:3004").replace(/\/$/, "");
const SOURCE_AGENT_ID = process.env.HERMES_TEAM_AGENT_ID || "default";
const PROTOCOL_VERSION = "2024-11-05";
const REQUEST_TIMEOUT_MS = 16 * 60 * 1000;

// The team token is delivered to this MCP only; it is never printed, logged, or returned in tool output.
function loadTeamToken() {
  const direct = (process.env.HERMES3D_TEAM_TOKEN || "").trim();
  if (direct) return direct;
  const file = (process.env.HERMES3D_TEAM_TOKEN_FILE || "").trim();
  if (!file) return "";
  try { return readFileSync(file, "utf8").trim(); } catch { return ""; }
}
const TEAM_TOKEN = loadTeamToken();

const tools = [
  {
    name: "team_roster",
    description: "List rostered specialists (id, name, role, model, status). Call this first when the work needs a teammate, then choose the specialist whose role fits. Lucy (id default) is the coordinator and is never a delegation target.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "team_delegate",
    description: "Delegate one substantive, self-contained task to a rostered specialist and return its exact run result. Give concise context and concrete deliverables; the specialist investigates with its own tools, so do not pre-investigate for it. Skip trivial questions. Never delegate to yourself (Lucy).",
    inputSchema: { type: "object", properties: { agent: { type: "string", description: "Specialist agent ID from team_roster (not Lucy)." }, task: { type: "string" }, context: { type: "string" }, deliverables: { type: "array", items: { type: "string" } }, acceptance_criteria: { type: "string" } }, required: ["agent", "task"], additionalProperties: false },
  },
  {
    name: "team_delegate_many",
    description: "Dispatch independent tasks to several rostered specialists in parallel and wait for every result. Call this tool in the same turn as the decision to dispatch, with every assignment in one call; do not announce a dispatch without calling it. Use only for tasks that do not depend on each other. Keep each assignment's task and context to a few sentences; specialists use their own tools. Returns one result per assignment (completed, failed, or timeout). Never target Lucy.",
    inputSchema: { type: "object", properties: { assignments: { type: "array", minItems: 1, items: { type: "object", properties: { agent: { type: "string" }, task: { type: "string" }, context: { type: "string" }, deliverables: { type: "array", items: { type: "string" } }, acceptance_criteria: { type: "string" } }, required: ["agent", "task"], additionalProperties: false } } }, required: ["assignments"], additionalProperties: false },
  },
  {
    name: "team_message",
    description: "Send a short coordination message to a rostered specialist about ongoing work (never Lucy). For a new task, use team_delegate instead.",
    inputSchema: { type: "object", properties: { agent: { type: "string" }, message: { type: "string" } }, required: ["agent", "message"], additionalProperties: false },
  },
];

function error(message, code = -32600) {
  return { code, message };
}

async function request(path, method, payload) {
  if (!TEAM_TOKEN) throw new Error("Team API token is not configured for the hermes-team MCP. Set HERMES3D_TEAM_TOKEN_FILE.");
  const body = payload === undefined ? undefined : JSON.stringify(payload);
  const headers = { authorization: `Bearer ${TEAM_TOKEN}`, ...(body === undefined ? {} : { "content-type": "application/json", "content-length": Buffer.byteLength(body) }) };
  const response = await new Promise((resolve, reject) => {
    const req = httpRequest(`${API_BASE_URL}${path}`, { method, headers }, (res) => {
      const chunks = [];
      res.setEncoding("utf8");
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, text: chunks.join("") }));
      res.on("error", reject);
    });
    // Delegation holds the response open until every run finishes; the inactivity timeout must outlast a full run.
    req.setTimeout(REQUEST_TIMEOUT_MS, () => req.destroy(new Error("Team API request timed out.")));
    req.on("error", reject);
    if (body !== undefined) req.write(body);
    req.end();
  });
  let data;
  try { data = response.text ? JSON.parse(response.text) : null; } catch { data = response.text; }
  if (response.status < 200 || response.status >= 300) throw new Error(`Team API ${response.status}: ${typeof data === "string" ? data : JSON.stringify(data)}`);
  return data;
}

const post = (path, payload) => request(path, "POST", payload);

function assertNotSelf(agent) {
  if (typeof agent !== "string" || !agent.trim()) throw new Error("agent must be a non-empty agent ID");
  if (agent.trim().toLowerCase() === SOURCE_AGENT_ID.toLowerCase()) throw new Error("The source agent cannot target itself");
}


async function callTool(name, args) {
  if (name === "team_roster") return request("/api/team/roster", "GET");
  if (name === "team_delegate") {
    assertNotSelf(args.agent);
    return post("/api/team/delegate", {
      targetAgentId: args.agent,
      task: args.task,
      ...(args.context !== undefined && { context: args.context }),
      ...(args.deliverables !== undefined && { deliverables: args.deliverables }),
      ...(args.acceptance_criteria !== undefined && { acceptanceCriteria: args.acceptance_criteria }),
      sourceAgentId: SOURCE_AGENT_ID,
    });
  }
  if (name === "team_delegate_many") {
    if (!Array.isArray(args.assignments) || args.assignments.length === 0) throw new Error("assignments must contain at least one task");
    const seen = new Set();
    const assignments = args.assignments.map((assignment) => {
      assertNotSelf(assignment.agent);
      const id = assignment.agent.trim().toLowerCase();
      if (seen.has(id)) throw new Error(`Duplicate agent assignment: ${assignment.agent}`);
      seen.add(id);
      return {
        targetAgentId: assignment.agent,
        task: assignment.task,
        ...(assignment.context !== undefined && { context: assignment.context }),
        ...(assignment.deliverables !== undefined && { deliverables: assignment.deliverables }),
        ...(assignment.acceptance_criteria !== undefined && { acceptanceCriteria: assignment.acceptance_criteria }),
        sourceAgentId: SOURCE_AGENT_ID,
      };
    });
    return post("/api/team/delegate-many", { assignments });
  }
  if (name === "team_message") {
    assertNotSelf(args.agent);
    return post("/api/team/message", { targetAgentId: args.agent, message: args.message, sourceAgentId: SOURCE_AGENT_ID });
  }
  throw new Error(`Unknown tool: ${name}`);
}

async function handle(message) {
  if (!message || typeof message !== "object" || Array.isArray(message)) return null;
  if (message.method === "notifications/initialized" || message.method?.startsWith("notifications/")) return null;
  if (message.method === "initialize") {
    return { jsonrpc: "2.0", id: message.id, result: { protocolVersion: PROTOCOL_VERSION, capabilities: { tools: {} }, serverInfo: { name: "hermes-team", version: "1.0.0" } } };
  }
  if (message.method === "ping") return { jsonrpc: "2.0", id: message.id, result: {} };
  if (message.method === "tools/list") return { jsonrpc: "2.0", id: message.id, result: { tools } };
  if (message.method === "tools/call") {
    try {
      const value = await callTool(message.params?.name, message.params?.arguments ?? {});
      return { jsonrpc: "2.0", id: message.id, result: { content: [{ type: "text", text: typeof value === "string" ? value : JSON.stringify(value ?? null) }], isError: false } };
    } catch (cause) {
      return { jsonrpc: "2.0", id: message.id, result: { content: [{ type: "text", text: cause instanceof Error ? cause.message : String(cause) }], isError: true } };
    }
  }
  if (message.id === undefined) return null;
  return { jsonrpc: "2.0", id: message.id, error: error(`Method not found: ${message.method}`, -32601) };
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  for (;;) {
    const newline = buffer.indexOf("\n");
    if (newline < 0) break;
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (!line.trim()) continue;
    let message;
    try { message = JSON.parse(line); } catch {
      process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id: null, error: error("Parse error", -32700) })}\n`);
      continue;
    }
    Promise.resolve(handle(message)).then((response) => {
      if (response) process.stdout.write(`${JSON.stringify(response)}\n`);
    }).catch((cause) => {
      if (message.id !== undefined) process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id: message.id, error: error(String(cause)) })}\n`);
    });
  }
});
