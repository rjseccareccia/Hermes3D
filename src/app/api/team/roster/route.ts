import { NextResponse } from "next/server";
import { listTeam, withTeamGateway } from "@/lib/runtime/teamOrchestration";
import { checkTeamAuthorization, teamAuthErrorResponse } from "@/lib/runtime/teamAuth";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const auth = checkTeamAuthorization(request);
  if (!auth.ok) return teamAuthErrorResponse(auth);
  try {
    return NextResponse.json(await withTeamGateway(listTeam), { headers: { "Cache-Control": "no-store" } });
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : "Failed to read team roster." }, { status: 502 });
  }
}
