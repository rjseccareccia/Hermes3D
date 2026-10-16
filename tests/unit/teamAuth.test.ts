// @vitest-environment node
import { describe, expect, it } from "vitest";

import { checkTeamAuthorization, teamAuthErrorResponse, TEAM_TOKEN_ENV } from "@/lib/runtime/teamAuth";

const TEAM_TOKEN = "team-token-for-unit-tests-0001";
const GATEWAY_TOKEN = "gateway-token-for-unit-tests-0002";
const ENV = { [TEAM_TOKEN_ENV]: TEAM_TOKEN, HERMES3D_GATEWAY_TOKEN: GATEWAY_TOKEN };

const teamRequest = (authorization?: string) =>
  new Request("http://studio.test/api/team/roster", authorization === undefined ? {} : { headers: { authorization } });

describe("team API authorization", () => {
  it("rejects a request without credentials with 401", () => {
    expect(checkTeamAuthorization(teamRequest(), ENV)).toEqual({
      ok: false,
      status: 401,
      error: "Team API requires a bearer token.",
    });
  });

  it("rejects a non-bearer scheme with 401", () => {
    expect(checkTeamAuthorization(teamRequest(`Basic ${TEAM_TOKEN}`), ENV)).toMatchObject({ ok: false, status: 401 });
  });

  it("rejects a wrong token with 403", () => {
    expect(checkTeamAuthorization(teamRequest("Bearer not-the-token"), ENV)).toEqual({
      ok: false,
      status: 403,
      error: "Team API token is invalid.",
    });
  });

  it("does not accept the Hermes gateway token as a team credential", () => {
    expect(checkTeamAuthorization(teamRequest(`Bearer ${GATEWAY_TOKEN}`), ENV)).toMatchObject({ ok: false, status: 403 });
  });

  it("accepts the configured team token, with a case-insensitive scheme", () => {
    expect(checkTeamAuthorization(teamRequest(`bearer ${TEAM_TOKEN}`), ENV)).toEqual({ ok: true });
  });

  it("fails closed with 503 when no team token is configured, even for a presented token", () => {
    expect(checkTeamAuthorization(teamRequest(`Bearer ${TEAM_TOKEN}`), { HERMES3D_GATEWAY_TOKEN: GATEWAY_TOKEN })).toMatchObject({
      ok: false,
      status: 503,
    });
  });

  it("answers a denial with its status, a no-store header, and a bearer challenge only for 401", async () => {
    const unauthenticated = teamAuthErrorResponse({ ok: false, status: 401, error: "Team API requires a bearer token." });
    expect(unauthenticated.status).toBe(401);
    expect(unauthenticated.headers.get("www-authenticate")).toContain("Bearer");
    expect(unauthenticated.headers.get("cache-control")).toBe("no-store");
    expect(await unauthenticated.json()).toEqual({ error: "Team API requires a bearer token." });

    const forbidden = teamAuthErrorResponse({ ok: false, status: 403, error: "Team API token is invalid." });
    expect(forbidden.status).toBe(403);
    expect(forbidden.headers.get("www-authenticate")).toBeNull();
  });
});
