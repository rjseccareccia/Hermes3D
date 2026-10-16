// @vitest-environment node
import { afterEach, describe, expect, it } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocketServer, type WebSocket as WsSocket } from "ws";
import type { AddressInfo } from "node:net";
import { createSessionSelectionStore } from "../../server/hermes-agent/session-selection";

const { createHermesAgentUpstream } = await import("../../server/hermes-agent/bridge");

type Frame = Record<string, unknown>;
type Handler = (params: Frame) => Frame | void;
const servers: WebSocketServer[] = [];
const upstreams: { terminate: () => void }[] = [];
const dirs: string[] = [];

afterEach(async () => {
  for (const upstream of upstreams.splice(0)) upstream.terminate();
  for (const server of servers.splice(0)) await new Promise<void>((resolve) => server.close(() => resolve()));
  for (const dir of dirs.splice(0)) await rm(dir, { recursive: true, force: true });
});

const fakeBackend = async (handlers: Record<string, Handler>) => {
  const server = new WebSocketServer({ port: 0 });
  servers.push(server);
  await new Promise<void>((resolve) => server.on("listening", resolve));
  const requests: Frame[] = [];
  server.on("connection", (socket: WsSocket) => {
    const send = (body: Frame) => socket.send(JSON.stringify(body));
    send({ jsonrpc: "2.0", method: "event", params: { type: "gateway.ready", payload: {} } });
    socket.on("message", (raw) => {
      const request = JSON.parse(String(raw)) as Frame;
      requests.push(request);
      const method = String(request.method);
      try {
        const result = handlers[method]?.((request.params ?? {}) as Frame);
        send({ jsonrpc: "2.0", id: request.id, result: result ?? {} });
      } catch (error) {
        send({ jsonrpc: "2.0", id: request.id, error: { code: -32000, message: error instanceof Error ? error.message : String(error) } });
      }
    });
  });
  return { url: `ws://127.0.0.1:${(server.address() as AddressInfo).port}`, requests };
};

const open = async (url: string, selectionPath: string) => {
  const upstream = createHermesAgentUpstream({ url, sessionSelectionPath: selectionPath });
  upstreams.push(upstream);
  const frames: Frame[] = [];
  upstream.on("message", (raw: string) => frames.push(JSON.parse(raw) as Frame));
  await new Promise<void>((resolve, reject) => {
    upstream.once("open", resolve);
    upstream.once("error", reject);
  });
  const bridge = {
    close: () => upstream.close(1000, "reconnect"),
    send: (frame: Frame) => upstream.send(JSON.stringify(frame)),
    async response(id: string) {
      const start = Date.now();
      while (Date.now() - start < 5000) {
        const found = frames.find((frame) => frame.type === "res" && frame.id === id);
        if (found) return found;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      throw new Error(`Timed out waiting for ${id}: ${JSON.stringify(frames)}`);
    },
  };
  bridge.send({ type: "req", id: "connect", method: "connect", params: {} });
  await bridge.response("connect");
  return bridge;
};

const tempSelection = async () => {
  const dir = await mkdtemp(join(tmpdir(), "hermes-hydration-"));
  dirs.push(dir);
  return join(dir, "selection.json");
};
const profileRows = [{ name: "default", is_default: true }, { name: "work" }];
const setup = async (handlers: Record<string, Handler>) => {
  const backend = await fakeBackend({ "profiles.list": () => ({ profiles: profileRows }), ...handlers });
  const selectionPath = await tempSelection();
  return { backend, selectionPath };
};
const request = async (bridge: Awaited<ReturnType<typeof open>>, id: string, method: string, params: Frame = {}) => {
  bridge.send({ type: "req", id, method, params });
  return bridge.response(id);
};
const sendPrompt = (bridge: Awaited<ReturnType<typeof open>>, id: string, key = "agent:default:main") =>
  request(bridge, id, "chat.send", { sessionKey: key, message: "hello" });

describe("persistent Hermes profile hydration", () => {
  it("resumes an existing profile main session rather than creating", async () => {
    const { backend, selectionPath } = await setup({
      "session.list": () => ({ sessions: [{ id: "old", source: "prompt", started_at: 1 }] }),
      "session.resume": () => ({ session_id: "runtime-old", session_key: "old" }),
      "prompt.submit": () => ({}),
    });
    const bridge = await open(backend.url, selectionPath);
    expect((await sendPrompt(bridge, "send")).ok).toBe(true);
    expect(backend.requests.find((r) => r.method === "session.resume")?.params).toMatchObject({ session_id: "old", lazy: true, omit_messages: true });
    expect(backend.requests.some((r) => r.method === "session.create")).toBe(false);
  });

  it("uses backend last-active row order, not started_at, when selecting main", async () => {
    const { backend, selectionPath } = await setup({
      "session.list": () => ({ sessions: [{ id: "recent", source: "prompt", started_at: 1 }, { id: "ancient", source: "prompt", started_at: 999999 }] }),
      "session.resume": () => ({ session_id: "runtime", session_key: "recent" }), "prompt.submit": () => ({}),
    });
    await sendPrompt(await open(backend.url, selectionPath), "send");
    expect(backend.requests.find((r) => r.method === "session.resume")?.params).toMatchObject({ session_id: "recent" });
  });

  it("keeps an empty profile empty until a prompt creates its first session", async () => {
    let creates = 0;
    const { backend, selectionPath } = await setup({ "session.list": () => ({ sessions: [] }), "session.create": () => { creates++; return { session_id: "new", stored_session_id: "new-stored" }; }, "prompt.submit": () => ({}) });
    const bridge = await open(backend.url, selectionPath);
    const history = await request(bridge, "history", "chat.history");
    expect(history).toMatchObject({ ok: true, payload: { messages: [] } });
    expect(creates).toBe(0);
    expect((await sendPrompt(bridge, "send")).ok).toBe(true);
    expect(creates).toBe(1);
  });

  it("hydrates history before any prompt", async () => {
    const { backend, selectionPath } = await setup({
      "session.list": () => ({ sessions: [{ id: "saved", source: "prompt" }] }),
      "session.resume": () => ({ session_id: "runtime", session_key: "saved" }),
      "session.history": () => ({ messages: [{ role: "user", text: "prior" }] }),
    });
    const response = await request(await open(backend.url, selectionPath), "history", "chat.history");
    expect(response).toMatchObject({ ok: true, payload: { messages: [{ role: "user", content: "prior" }] } });
    expect(backend.requests.some((r) => r.method === "prompt.submit")).toBe(false);
  });

  it("resumes a main session lazily for preview after bridge reconnect", async () => {
    const { backend, selectionPath } = await setup({
      "session.list": () => ({ sessions: [{ id: "saved", source: "prompt" }] }),
      "session.resume": () => ({ session_id: "runtime", session_key: "saved" }),
      "session.history": () => ({ messages: [{ role: "assistant", text: "preview" }] }),
    });
    const first = await open(backend.url, selectionPath);
    await request(first, "history", "chat.history");
    const second = await open(backend.url, selectionPath);
    const preview = await request(second, "preview", "sessions.preview", { keys: ["agent:default:main"] });
    expect(preview).toMatchObject({ payload: { previews: [{ key: "agent:default:main", status: "ok", items: [{ text: "preview" }] }] } });
    expect(backend.requests.filter((r) => r.method === "session.resume")).toHaveLength(2);
  });

  it("restores a pinned selection in a fresh bridge and deduplicates it from stored sessions", async () => {
    let rows = [{ id: "chosen", source: "cron" }, { id: "other", source: "tui" }];
    const { backend, selectionPath } = await setup({ "session.list": () => ({ sessions: rows }), "session.resume": (p) => ({ session_id: `runtime-${p.session_id}`, session_key: p.session_id }), "prompt.submit": () => ({}) });
    createSessionSelectionStore(backend.url, selectionPath).set("default", { storedId: "chosen" });
    const first = await open(backend.url, selectionPath);
    await sendPrompt(first, "one");
    rows = [{ id: "other", source: "tui" }, { id: "new-cron", source: "cron" }, { id: "chosen", source: "cron" }];
    const second = await open(backend.url, selectionPath);
    await sendPrompt(second, "two");
    expect(backend.requests.filter((r) => r.method === "session.resume").at(-1)?.params).toMatchObject({ session_id: "chosen" });
    const listed = await request(second, "listed", "sessions.list");
    expect((listed.payload as Frame).sessions).not.toEqual(expect.arrayContaining([expect.objectContaining({ key: "agent:default:chosen" })]));
  });

  it("restores an empty reset draft across bridge reconnection", async () => {
    const { backend, selectionPath } = await setup({
      "session.list": () => ({ sessions: [{ id: "old", source: "prompt" }] }),
      "session.create": () => ({ session_id: "draft-runtime", stored_session_id: "draft-stored" }),
      "session.resume": (p) => ({ session_id: `runtime-${p.session_id}`, session_key: p.session_id }),
      "session.history": () => ({ messages: [{ role: "user", text: "must not show" }] }),
      "prompt.submit": () => ({}),
    });
    const first = await open(backend.url, selectionPath);
    await request(first, "reset", "sessions.reset");
    const second = await open(backend.url, selectionPath);
    const history = await request(second, "history", "chat.history");
    expect(history).toMatchObject({ ok: true, payload: { messages: [] } });
    expect(backend.requests.filter((r) => r.method === "session.resume")).toHaveLength(0);
    await sendPrompt(second, "draft");
    expect(backend.requests.filter((r) => r.method === "session.resume").at(-1)?.params).toMatchObject({ session_id: "draft-stored", lazy: false });
    expect(backend.requests.filter((r) => r.method === "session.create")).toHaveLength(1);
  });

  it("passes the selected named profile to list, resume, and history", async () => {
    const { backend, selectionPath } = await setup({ "session.list": () => ({ sessions: [{ id: "work-saved", source: "prompt" }] }), "session.resume": () => ({ session_id: "runtime", session_key: "work-saved" }), "session.history": () => ({ messages: [] }) });
    await request(await open(backend.url, selectionPath), "history", "chat.history", { sessionKey: "agent:work:main" });
    for (const call of backend.requests.filter((r) => ["session.list", "session.resume", "session.history"].includes(String(r.method)))) expect(call.params).toMatchObject({ profile: "work" });
  });

  it("isolates colliding stored IDs between profiles", async () => {
    const { backend, selectionPath } = await setup({ "session.list": (p) => ({ sessions: [{ id: "shared", source: "prompt", title: String(p.profile || "default") }] }), "session.resume": (p) => ({ session_id: `runtime-${p.profile || "default"}`, session_key: "shared" }), "session.history": (p) => ({ messages: [{ role: "assistant", text: String(p.session_id) }] }) });
    const bridge = await open(backend.url, selectionPath);
    await request(bridge, "d", "chat.history", { sessionKey: "agent:default:main" });
    await request(bridge, "w", "chat.history", { sessionKey: "agent:work:main" });
    expect(backend.requests.filter((r) => r.method === "session.resume").map((r) => r.params)).toEqual(expect.arrayContaining([expect.objectContaining({ session_id: "shared" }), expect.objectContaining({ session_id: "shared", profile: "work" })]));
  });

  it("rejects a stored ID that exists only in another profile", async () => {
    const { backend, selectionPath } = await setup({
      "session.list": (p) => ({ sessions: p.profile === "work" ? [{ id: "work-only", source: "prompt" }] : [{ id: "foreign", source: "prompt" }] }),
      "session.resume": () => ({ session_id: "should-not-resume" }),
      "session.create": () => ({ session_id: "should-not-create" }),
    });
    const response = await request(await open(backend.url, selectionPath), "foreign", "chat.history", { sessionKey: "agent:work:foreign" });
    expect(response.ok).toBe(false);
    expect(backend.requests.some((r) => r.method === "session.resume" || r.method === "session.create")).toBe(false);
  });

  it("reset creates and pins only the selected profile main through reconnect", async () => {
    let created = 0;
    const { backend, selectionPath } = await setup({
      "session.list": (p) => ({ sessions: [{ id: "old", source: "prompt" }, { id: "work-old", source: "prompt" }].filter((row) => p.profile === "work" ? row.id.startsWith("work") : row.id === "old") }),
      "session.create": (p) => ({ session_id: `runtime-${++created}`, stored_session_id: p.profile === "work" ? "work-new" : "new" }),
      "session.resume": (p) => ({ session_id: `runtime-${p.session_id}`, session_key: p.session_id }),
      "prompt.submit": () => ({}),
    });
    const bridge = await open(backend.url, selectionPath);
    await sendPrompt(bridge, "default");
    await sendPrompt(bridge, "work", "agent:work:main");
    await request(bridge, "reset", "sessions.reset", { key: "agent:default:main" });
    expect(backend.requests.filter((r) => r.method === "session.create")).toHaveLength(1);
    bridge.close();
    const reconnect = await open(backend.url, selectionPath);
    await sendPrompt(reconnect, "after");
    expect(backend.requests.filter((r) => r.method === "session.resume").at(-1)?.params).toMatchObject({ session_id: "new", lazy: false });
    await sendPrompt(reconnect, "work-after", "agent:work:main");
    expect(backend.requests.filter((r) => r.method === "session.resume").at(-1)?.params).toMatchObject({ session_id: "work-old", profile: "work" });
    expect(backend.requests.some((r) => r.method === "session.close")).toBe(false);
  });

  it("keeps a newly created durable main pinned despite newer cron and interactive activity", async () => {
    let rows = [{ id: "old", source: "tui" }];
    const { backend, selectionPath } = await setup({
      "session.list": () => ({ sessions: rows }),
      "session.create": () => ({ session_id: "new-runtime", stored_session_id: "new-main" }),
      "session.resume": (p) => ({ session_id: `runtime-${p.session_id}`, session_key: p.session_id }),
      "prompt.submit": () => {
        rows = [
          { id: "newer-cron", source: "cron" },
          { id: "newer-interactive", source: "desktop" },
          { id: "new-main", source: "tui" },
          { id: "old", source: "tui" },
        ];
        return {};
      },
    });
    const first = await open(backend.url, selectionPath);
    await request(first, "reset", "sessions.reset", { key: "agent:default:main" });
    await sendPrompt(first, "persist-new");
    first.close();
    await sendPrompt(await open(backend.url, selectionPath), "reconnect-new");
    expect(backend.requests.filter((r) => r.method === "session.resume").at(-1)?.params).toMatchObject({ session_id: "new-main" });
  });

  it("keeps an older explicitly keyed history accessible after main reset", async () => {
    const { backend, selectionPath } = await setup({ "session.list": () => ({ sessions: [{ id: "old", source: "prompt" }] }), "session.resume": (p) => ({ session_id: `runtime-${p.session_id}`, session_key: p.session_id }), "session.create": () => ({ session_id: "new-runtime", stored_session_id: "new" }), "session.history": (p) => ({ messages: [{ role: "user", text: String(p.session_id) }] }) });
    const bridge = await open(backend.url, selectionPath);
    await request(bridge, "reset", "sessions.reset", { key: "agent:default:main" });
    const history = await request(bridge, "old-history", "chat.history", { sessionKey: "agent:default:old" });
    expect(history).toMatchObject({ payload: { messages: [{ content: "runtime-old" }] } });
  });

  it("falls back when the persisted selection was deleted", async () => {
    let rows = [{ id: "pinned", source: "tui" }, { id: "fallback", source: "desktop" }];
    const { backend, selectionPath } = await setup({
      "session.list": () => ({ sessions: rows }),
      "session.resume": (p) => ({ session_id: `runtime-${p.session_id}`, session_key: p.session_id }),
      "session.history": () => ({ messages: [] }),
      "prompt.submit": () => ({}),
    });
    const first = await open(backend.url, selectionPath);
    await request(first, "history", "chat.history");
    rows = [{ id: "cron", source: "cron" }, { id: "api", source: "api_server" }, { id: "fallback", source: "desktop" }];
    const reconnect = await open(backend.url, selectionPath);
    await sendPrompt(reconnect, "fallback");
    expect(backend.requests.filter((r) => r.method === "session.resume").at(-1)?.params).toMatchObject({ session_id: "fallback" });
    rows.unshift({ id: "new-interactive", source: "cli" });
    await sendPrompt(await open(backend.url, selectionPath), "persisted-replacement");
    expect(backend.requests.filter((r) => r.method === "session.resume").at(-1)?.params).toMatchObject({ session_id: "fallback" });
  });

  it("reports resume failures in history and preview without creating", async () => {
    const { backend, selectionPath } = await setup({ "session.list": () => ({ sessions: [{ id: "broken", source: "prompt" }] }), "session.resume": () => { throw new Error("resume failed"); } });
    const bridge = await open(backend.url, selectionPath);
    const history = await request(bridge, "history", "chat.history");
    expect(history.ok).toBe(false);
    const preview = await request(bridge, "preview", "sessions.preview", { keys: ["agent:default:main"] });
    expect(preview).toMatchObject({ payload: { previews: [{ status: "error", error: expect.stringContaining("resume failed") }] } });
    expect(backend.requests.some((r) => r.method === "session.create")).toBe(false);
  });

  it("rejects unknown profiles without creating a session", async () => {
    const { backend, selectionPath } = await setup({ "session.list": () => ({ sessions: [] }), "session.create": () => ({ session_id: "must-not-create" }) });
    const response = await sendPrompt(await open(backend.url, selectionPath), "unknown", "agent:missing:main");
    expect(response.ok).toBe(false);
    expect(backend.requests.some((r) => r.method === "session.create")).toBe(false);
  });
  it("follows the pinned compression lineage instead of a newer unrelated conversation", async () => {
    let rows = [{ id: "tip-one", _lineage_root_id: "root", source: "tui" }];
    const { backend, selectionPath } = await setup({
      "session.list": () => ({ sessions: rows }),
      "session.resume": (p) => ({ session_id: `runtime-${p.session_id}`, session_key: p.session_id }),
      "session.history": (p) => ({ messages: [{ role: "assistant", text: String(p.session_id) }] }),
    });
    await request(await open(backend.url, selectionPath), "first", "chat.history");
    rows = [
      { id: "unrelated", _lineage_root_id: "", source: "tui" },
      { id: "tip-two", _lineage_root_id: "root", source: "tui" },
    ];
    const restored = await request(await open(backend.url, selectionPath), "restored", "chat.history");
    expect(restored).toMatchObject({ payload: { messages: [{ content: "runtime-tip-two" }] } });
  });
});
