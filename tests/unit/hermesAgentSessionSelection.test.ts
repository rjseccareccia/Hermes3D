// @vitest-environment node
import { describe, expect, it } from "vitest";

import { selectInitialSession } from "../../server/hermes-agent/session-selection";

const row = (id: string, source: string, started_at = 0) => ({ id, source, started_at });

describe("initial Hermes main-session ranking", () => {
  it.each(["cli", "tui", "desktop", "webui", "discord", "telegram"])(
    "%s interactive sessions beat newer cron and API sessions",
    (source) => {
      const interactive = row("interactive", source, 1);
      expect(selectInitialSession([
        row("new-cron", "cron", 999), row("api", "api_server", 998), interactive,
      ])).toBe(interactive);
    },
  );

  it.each(["a2a", "tool", "kanban", "oneshot", "webhook"])(
    "interactive sessions beat newer %s background sessions",
    (source) => {
      const interactive = row("interactive", "tui");
      expect(selectInitialSession([row("background", source), interactive])).toBe(interactive);
    },
  );

  it("preserves activity order among interactive sessions, not started_at order", () => {
    const active = row("most-active", "desktop", 1);
    expect(selectInitialSession([row("cron", "cron"), active, row("newer-start", "cli", 999)])).toBe(active);
  });

  it("selects the first non-background session when no known interactive source exists", () => {
    const api = row("api", "api_server", 1);
    expect(selectInitialSession([row("cron", "cron"), row("delegated", "a2a"), api, row("later-api", "api_server", 999)])).toBe(api);
  });

  it("falls back to first activity-ordered row when every session is automated", () => {
    const latest = row("latest", "cron", 1);
    expect(selectInitialSession([latest, row("newer-start", "a2a", 999)])).toBe(latest);
  });

  it("keeps profiles without sessions empty", () => {
    expect(selectInitialSession([])).toBeNull();
  });

  it("uses canonical source instead of ID or title guesses", () => {
    const chat = { ...row("cron_looks-automated", "desktop"), title: "Cron review" };
    expect(selectInitialSession([{ ...row("normal-id", "cron"), title: "Interactive chat" }, chat])).toBe(chat);
  });

  it("does not promote missing or unknown provenance above proven interactive use", () => {
    const chat = row("chat", "tui");
    expect(selectInitialSession([{ id: "missing" }, row("guard-placeholder", "unknown"), chat])).toBe(chat);
  });
});
