import { createHash, timingSafeEqual } from "node:crypto";
import { NextResponse } from "next/server";

/**
 * Shared secret for the Lucy team MCP. It is deliberately separate from the
 * Hermes gateway token and is never returned by any team route.
 */
export const TEAM_TOKEN_ENV = "HERMES3D_TEAM_TOKEN";

type TeamAuthAllowed = { ok: true };
type TeamAuthDenied = { ok: false; status: 401 | 403 | 503; error: string };
export type TeamAuthResult = TeamAuthAllowed | TeamAuthDenied;

/**
 * Check the `Authorization: Bearer <token>` header against HERMES3D_TEAM_TOKEN.
 * Fails closed: with no configured token every request is refused (503).
 * Digests are compared in constant time so the comparison leaks no prefix.
 */
export function checkTeamAuthorization(request: Request, env: Readonly<Record<string, string | undefined>> = process.env): TeamAuthResult {
  const expected = env[TEAM_TOKEN_ENV]?.trim() ?? "";
  if (!expected) {
    return { ok: false, status: 503, error: `Team API is disabled: ${TEAM_TOKEN_ENV} is not configured.` };
  }
  const match = /^Bearer\s+(\S+)\s*$/i.exec(request.headers.get("authorization") ?? "");
  if (!match) {
    return { ok: false, status: 401, error: "Team API requires a bearer token." };
  }
  const presented = createHash("sha256").update(match[1], "utf8").digest();
  const configured = createHash("sha256").update(expected, "utf8").digest();
  return timingSafeEqual(presented, configured)
    ? { ok: true }
    : { ok: false, status: 403, error: "Team API token is invalid." };
}

export function teamAuthErrorResponse(denied: TeamAuthDenied) {
  return NextResponse.json(
    { error: denied.error },
    {
      status: denied.status,
      headers: {
        "Cache-Control": "no-store",
        ...(denied.status === 401 ? { "WWW-Authenticate": 'Bearer realm="hermes3d-team"' } : {}),
      },
    },
  );
}
