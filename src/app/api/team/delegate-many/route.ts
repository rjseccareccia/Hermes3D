import { NextResponse } from "next/server";
import { delegateMany, withTeamGateway } from "@/lib/runtime/teamOrchestration";
import { checkTeamAuthorization, teamAuthErrorResponse } from "@/lib/runtime/teamAuth";

export const runtime = "nodejs";
type Body = { assignments?: unknown };

export async function POST(request: Request) {
  const auth = checkTeamAuthorization(request);
  if (!auth.ok) return teamAuthErrorResponse(auth);
  let body: Body;
  try { body = await request.json() as Body; } catch { return NextResponse.json({ error: "Invalid JSON body." }, { status: 400 }); }
  if (!Array.isArray(body.assignments) || !body.assignments.every((item) => item && typeof item === "object" && "targetAgentId" in item && typeof item.targetAgentId === "string" && "task" in item && typeof item.task === "string")) return NextResponse.json({ error: "Assignments with target agent IDs and tasks are required." }, { status: 400 });
  const assignments = body.assignments.map((item) => {
    const assignment = item as { targetAgentId: string; task: string; context?: unknown; deliverables?: unknown; acceptanceCriteria?: unknown; sourceAgentId?: unknown; timeoutMs?: unknown };
    return {
      agentId: assignment.targetAgentId, task: assignment.task,
      context: typeof assignment.context === "string" ? assignment.context : undefined,
      deliverables: Array.isArray(assignment.deliverables) ? assignment.deliverables.filter((value): value is string => typeof value === "string") : undefined,
      acceptanceCriteria: typeof assignment.acceptanceCriteria === "string" ? assignment.acceptanceCriteria : undefined,
      sourceAgentId: typeof assignment.sourceAgentId === "string" ? assignment.sourceAgentId : undefined,
      timeoutMs: typeof assignment.timeoutMs === "number" && Number.isFinite(assignment.timeoutMs) ? Math.min(600_000, Math.max(1_000, assignment.timeoutMs)) : 600_000,
    };
  });
  try {
    const results = await withTeamGateway((client) => delegateMany(client, { assignments }));
    return NextResponse.json({ results }, { headers: { "Cache-Control": "no-store" } });
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : "Delegation failed." }, { status: 502 });
  }
}
