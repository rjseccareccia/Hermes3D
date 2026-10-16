const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const { resolveStudioSettingsPath } = require("../studio-settings");

// Hermes session.list exposes source, but no per-row interactive/automation
// flag. Recognized chat surfaces are strongest evidence of user-driven use.
const INTERACTIVE_SOURCES = new Set([
  "cli", "tui", "desktop", "webui", "local",
  "telegram", "discord", "whatsapp", "whatsapp_cloud", "slack", "signal",
  "mattermost", "matrix", "email", "sms", "dingtalk", "feishu", "wecom",
  "wecom_callback", "weixin", "bluebubbles", "qqbot", "yuanbao",
]);
const BACKGROUND_SOURCES = new Set([
  "cron", "a2a", "kanban", "tool", "oneshot", "webhook", "msgraph_webhook",
  "unknown",
]);

/** Select only within one profile's Hermes activity-ordered session.list rows. */
const selectInitialSession = (sessions) => {
  let selected = null;
  let bestRank = Infinity;
  for (const session of sessions) {
    const source = String(session.source ?? "").trim().toLowerCase();
    // API/missing/unrecognized sources are not proven interactive. They may
    // still beat known automation, but never a recognized interactive surface.
    const rank = INTERACTIVE_SOURCES.has(source) ? 0 : BACKGROUND_SOURCES.has(source) ? 2 : 1;
    if (rank < bestRank) {
      selected = session;
      bestRank = rank;
      if (rank === 0) break;
    }
  }
  return selected;
};

// Only identities live here. Hermes remains the sole transcript store.
const defaultSelectionPath = () => path.join(path.dirname(resolveStudioSettingsPath()), "hermes-agent-sessions.json");
const readSelections = (file) => {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return {};
    throw error;
  }
};

const createSessionSelectionStore = (url, file = defaultSelectionPath()) => {
  const endpoint = new URL(url);
  endpoint.search = "";
  endpoint.hash = "";
  const namespace = endpoint.toString();
  return {
    get(profile) {
      return readSelections(file)[namespace]?.[profile];
    },
    set(profile, selection) {
      // Read/modify/rename stays synchronous: simultaneous connections cannot lose
      // another profile's selection within this server process.
      const data = readSelections(file);
      data[namespace] = { ...data[namespace], [profile]: selection };
      fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
      const temporary = `${file}.${randomUUID()}.tmp`;
      try {
        fs.writeFileSync(temporary, JSON.stringify(data), { mode: 0o600 });
        fs.renameSync(temporary, file);
      } finally {
        if (fs.existsSync(temporary)) fs.unlinkSync(temporary);
      }
    },
  };
};

module.exports = { createSessionSelectionStore, selectInitialSession };
