import { NextResponse } from "next/server";
import { delegateToAgent, withTeamGateway } from "@/lib/runtime/teamOrchestration";
import { checkTeamAuthorization, teamAuthErrorResponse } from "@/lib/runtime/teamAuth";

export const runtime = "nodejs";
type Body = { targetAgentId?: unknown; task?: unknown; context?: unknown; deliverables?: unknown; acceptanceCriteria?: unknown; sourceAgentId?: unknown; timeoutMs?: unknown };

export async function POST(request: Request) {
  const auth = checkTeamAuthorization(request);
  if (!auth.ok) return teamAuthErrorResponse(auth);
  let body: Body;
  try { body = await request.json() as Body; } catch { return NextResponse.json({ error: "Invalid JSON body." }, { status: 400 }); }
  if (typeof body.targetAgentId !== "string" || typeof body.task !== "string" || !body.targetAgentId.trim() || !body.task.trim()) return NextResponse.json({ error: "Target agent ID and task are required." }, { status: 400 });
  const timeoutMs = typeof body.timeoutMs === "number" && Number.isFinite(body.timeoutMs) ? Math.min(600_000, Math.max(1_000, body.timeoutMs)) : 600_000;
  try {
    const result = await withTeamGateway((client) => delegateToAgent(client, {
      agentId: body.targetAgentId as string, task: body.task as string,
      context: typeof body.context === "string" ? body.context : undefined,
      deliverables: Array.isArray(body.deliverables) ? body.deliverables.filter((item): item is string => typeof item === "string") : undefined,
      acceptanceCriteria: typeof body.acceptanceCriteria === "string" ? body.acceptanceCriteria : undefined,
      sourceAgentId: typeof body.sourceAgentId === "string" ? body.sourceAgentId : undefined, timeoutMs,
    }));
    return NextResponse.json(result, { headers: { "Cache-Control": "no-store" } });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Delegation failed.";
    const status = message.startsWith("Unknown team agent:") ? 404 : message.startsWith("Cannot ") ? 400 : 502;
    return NextResponse.json({ error: message }, { status });
  }
}
