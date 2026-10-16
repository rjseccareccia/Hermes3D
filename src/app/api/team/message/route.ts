import { NextResponse } from "next/server";
import { messageAgent, withTeamGateway } from "@/lib/runtime/teamOrchestration";
import { checkTeamAuthorization, teamAuthErrorResponse } from "@/lib/runtime/teamAuth";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const auth = checkTeamAuthorization(request);
  if (!auth.ok) return teamAuthErrorResponse(auth);
  let body: { targetAgentId?: unknown; message?: unknown; sourceAgentId?: unknown };
  try { body = await request.json(); } catch { return NextResponse.json({ error: "Invalid JSON body." }, { status: 400 }); }
  if (typeof body.targetAgentId !== "string" || typeof body.message !== "string" || !body.targetAgentId.trim() || !body.message.trim()) return NextResponse.json({ error: "Target agent ID and message are required." }, { status: 400 });
  try {
    const result = await withTeamGateway((client) => messageAgent(client, {
      agentId: body.targetAgentId as string, message: body.message as string,
      sourceAgentId: typeof body.sourceAgentId === "string" ? body.sourceAgentId : undefined,
    }));
    return NextResponse.json({ result }, { headers: { "Cache-Control": "no-store" } });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Message delivery failed.";
    const status = message.startsWith("Unknown team agent:") ? 404 : message.startsWith("Cannot ") ? 400 : 502;
    return NextResponse.json({ error: message }, { status });
  }
}
