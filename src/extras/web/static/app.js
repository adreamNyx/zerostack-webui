/* zerostack web ui frontend — WebSocket client for /ws.
   Protocol types live in src/extras/web/mod.rs (module docs). */

const $ = (id) => document.getElementById(id);
const stream = $("stream");
const thread = $("thread");
const input = $("input");

const S = {
  ws: null, connected: false, busy: false,
  session: null, models: [], prompts: [], modes: [], cwd: "",
  reasoning: false, toolNodes: new Map(), live: null, liveReason: null,
  ask: null, history: [], histIdx: -1, queue: [],
  loop: null, btwCard: null, mention: { items: [], idx: -1 },
  treeState: new Map(), attached: [],
};

/* ── helpers ─────────────────────────────────────────────── */
const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

function md(text) {
  try {
    if (window.marked) {
      const html = marked.parse(text, { breaks: true, mangle: false, headerIds: false });
      if (window.DOMPurify) return DOMPurify.sanitize(html);
      return html;
    }
  } catch (e) { /* fall through */ }
  return "<p>" + esc(text).replace(/\n/g, "<br>") + "</p>";
}
const fmtTok = (n) => n >= 1e6 ? (n / 1e6).toFixed(2) + "M" : n >= 1e3 ? (n / 1e3).toFixed(1) + "k" : String(n || 0);
const fmtCost = (c) => "¥" + Number(c || 0).toFixed(4);
const nearBottom = () => thread.scrollHeight - thread.scrollTop - thread.clientHeight < 160;
const scrollDown = (force) => { if (force || nearBottom()) thread.scrollTop = thread.scrollHeight; };

function el(tag, cls, html) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (html != null) n.innerHTML = html;
  return n;
}
function sysmsg(text, kind) {
  const n = el("div", "sysmsg" + (kind === "err" ? " err" : kind === "ok" ? " ok" : ""), esc(text));
  stream.appendChild(n); scrollDown(true); return n;
}

/* ── connection ──────────────────────────────────────────── */
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  S.ws = ws;
  ws.onopen = () => {
    S.connected = true;
    $("kv-ws").textContent = "已连接"; $("kv-ws").style.color = "var(--ok)";
  };
  ws.onclose = () => {
    if (!S.connected) return;
    S.connected = false; setBusy(false);
    $("kv-ws").textContent = "已断开"; $("kv-ws").style.color = "var(--err)";
    sysmsg("连接已断开，3 秒后重连…", "err");
    setTimeout(connect, 3000);
  };
  ws.onmessage = (ev) => { let m; try { m = JSON.parse(ev.data); } catch (e) { return; } handle(m); };
}
function send(obj) { if (S.ws && S.ws.readyState === 1) S.ws.send(JSON.stringify(obj)); }

/* ── server events ───────────────────────────────────────── */
function handle(m) {
  switch (m.type) {
    case "session_state": return onSessionState(m);
    case "ui_state": return onUiState(m);
    case "session_list": return renderSessions(m.sessions || []);
    case "token": return onToken(m.text || "");
    case "reasoning": return onReasoning(m.text || "");
    case "tool_call": return onToolCall(m);
    case "tool_result": return onToolResult(m);
    case "subagent": return appendSubagent(m.name || "subagent", m.summary || "");
    case "permission_ask": return onAsk(m);
    case "retrying": return sysmsg(`第 ${m.attempt}/${m.max} 次重试上游…`);
    case "usage": return onUsage(m);
    case "done": return onDone(m);
    case "turn_end": return onTurnEnd();
    case "error": setBusy(false); finalizeLive(); sysmsg(m.message || "未知错误", "err"); return;
    case "export": return download(m.filename, m.content);
    case "session_deleted": if (S.session && S.session.id === m.id) S.session = null; return;
    case "dir_list": return onDirList(m);
    case "path_matches": return onPathMatches(m.items || []);
    case "attached": return renderAttached(m.files || []);
    case "config_state": return renderConfig(m);
    case "loop_status": return onLoopStatus(m);
    case "worktree": return sysmsg(m.message || "worktree", m.ok ? "ok" : "err");
    case "btw_token": return onBtwToken(m.text || "");
    case "btw_tool": return onBtwTool(m.name || "tool");
    case "btw_done": return onBtwDone(m);
    case "btw_end": return onBtwEnd();
    default: return;
  }
}

function onSessionState(m) {
  S.session = m.session || null;
  S.models = m.models || [];
  renderModels();
  renderMessages(m.messages || []);
  if (S.session) updateStats(S.session);
}

function onUiState(m) {
  S.prompts = m.prompts || []; S.modes = m.modes || []; S.cwd = m.cwd || "";
  const sp = $("sel-prompt");
  sp.innerHTML = "";
  sp.appendChild(new Option("默认(code) · 跟随配置", ""));
  S.prompts.forEach((p) => sp.appendChild(new Option(promptLabel(p), p)));
  sp.value = m.prompt || "";
  const sm = $("sel-mode");
  sm.innerHTML = ""; S.modes.forEach((x) => sm.appendChild(new Option(modeLabel(x), x)));
  sm.value = m.mode || "standard";
  S.reasoning = !!m.reasoning;
  $("btn-reason").classList.toggle("on", S.reasoning);
  $("cwd-text").textContent = S.cwd || "—";
  $("btn-cwd").title = S.cwd || "";
}

/* 提示词模式:英文名 → [中文名, 该模式会强制切换到的权限模式(空=不改)]
   英文名保持原样(session.prompt / --prompt 用的就是它),中文只做展示。 */
const PROMPT_LABEL = {
  "code":            ["编码", ""],
  "default":         ["默认模式", ""],
  "plan":            ["规划", "只写"],
  "debug":           ["调试", ""],
  "review":          ["代码审查", "只读"],
  "review-security": ["安全审查", "只读"],
  "simplify":        ["简化代码", ""],
  "refactor":        ["重构", ""],
  "brainstorm":      ["头脑风暴", "只读"],
  "ask":             ["只读问答", "只读"],
  "autoconfig":      ["配置助手", "配置"],
  "frontend-design": ["前端设计", ""],
  "orchestrator":    ["编排调度", ""],
  "work":            ["办公文档", ""],
  "write-prompt":    ["写提示词", ""],
  "write-text":      ["写技术文本", ""],
};

function promptLabel(name) {
  const e = PROMPT_LABEL[name];
  if (!e) return name;
  return e[1] ? `${name} · ${e[0]} (${e[1]})` : `${name} · ${e[0]}`;
}

const MODE_LABEL = {
  standard: "standard · 目录内自动", guarded: "guarded · 读放行写要批",
  readonly: "readonly · 只读", restrictive: "restrictive · 全部要批",
  yolo: "yolo · 全放行(危险)", planwrite: "planwrite · 只写计划",
};
const modeLabel = (x) => MODE_LABEL[x] || x;

/* ── message rendering ───────────────────────────────────── */
function clearStream() { stream.innerHTML = ""; S.toolNodes.clear(); S.live = null; S.liveReason = null; }

function renderMessages(msgs) {
  clearStream();
  if (!msgs.length) stream.appendChild(emptyCard());
  else msgs.forEach(renderStored);
  $("st-msgs").textContent = msgs.length;
  scrollDown(true);
}
function emptyCard() {
  const n = el("div", "empty");
  n.innerHTML = `<div class="empty-logo">z</div><h1>zerostack web</h1>
    <p>Rust 写的极简编码 agent · WebSocket 前端</p>
    <div class="empty-tips"><span>Enter 发送</span><span>Shift+Enter 换行</span><span>@ 引用文件</span>
    <span>/loop 长任务</span><span>/btw 旁问</span><span>Esc 中止</span></div>`;
  return n;
}
function renderStored(m) {
  const role = m.role;
  if (role === "user") appendUser(m.content);
  else if (role === "assistant") {
    const n = el("div", "msg assistant");
    n.appendChild(el("div", "md", md(m.content)));
    stream.appendChild(n);
  } else if (role === "tool_call") {
    const t = m.tool || {};
    makeToolCard(t.name || "tool", m.content || "", t.id).classList.add("open");
  } else if (role === "tool_result") {
    const t = m.tool || {};
    const card = S.toolNodes.get(t.call_id) || S.toolNodes.get(String(t.call_id));
    if (card) {
      card.querySelector(".tool-state").classList.add("done");
      card.querySelector(".tool-body pre").textContent = m.content || "(无输出)";
    }
  } else if (role === "subagent_tool_call") {
    const t = m.tool || {}; appendSubagent(t.name || "subagent", m.content || "");
  } else if (role === "system") sysmsg(m.content);
}
function appendUser(text) {
  const n = el("div", "msg user");
  n.appendChild(el("div", "bubble", esc(text)));
  stream.appendChild(n); scrollDown(true);
}
function ensureLive() {
  if (S.live) return S.live;
  const n = el("div", "msg assistant");
  const body = el("div", "md");
  body.appendChild(el("span", "cursor"));
  n.appendChild(body); stream.appendChild(n);
  S.live = { node: n, body, buf: "" };
  return S.live;
}
function finalizeLive() {
  if (!S.live) return;
  if (S.live.buf.trim()) S.live.body.innerHTML = md(S.live.buf);
  else S.live.node.remove();
  S.live = null; S.liveReason = null; scrollDown();
}
let rafPending = false;
function onToken(text) {
  const live = ensureLive();
  live.buf += text;
  if (!rafPending) {
    rafPending = true;
    requestAnimationFrame(() => {
      rafPending = false;
      if (!S.live) return;
      const near = nearBottom();
      S.live.body.innerHTML = md(S.live.buf) + '<span class="cursor"></span>';
      if (near) scrollDown(true);
    });
  }
}
function onReasoning(text) {
  if (!S.liveReason) {
    const wrap = el("div", "reason");
    const head = el("div", "reason-head", "▾ 思考过程");
    const body = el("div", "reason-body");
    wrap.appendChild(head); wrap.appendChild(body);
    head.onclick = () => wrap.classList.toggle("collapsed");
    (S.live ? S.live.node : stream).appendChild(wrap);
    S.liveReason = { node: wrap, body, buf: "" };
  }
  S.liveReason.buf += text;
  S.liveReason.body.textContent = S.liveReason.buf.slice(-4000);
  if (nearBottom()) scrollDown();
}
function makeToolCard(name, summary, callId) {
  const card = el("div", "tool");
  const head = el("div", "tool-head");
  head.appendChild(el("span", "tool-state"));
  head.appendChild(el("span", "tool-name", esc(name)));
  head.appendChild(el("span", "tool-sum", esc(summary || "")));
  const body = el("div", "tool-body");
  body.appendChild(el("pre", null, ""));
  card.appendChild(head); card.appendChild(body);
  head.onclick = () => card.classList.toggle("open");
  stream.appendChild(card);
  if (callId != null) { S.toolNodes.set(callId, card); S.toolNodes.set(String(callId), card); }
  scrollDown();
  return card;
}
function onToolCall(m) { makeToolCard(m.name || "tool", m.summary || "", m.call_id); }
function onToolResult(m) {
  const card = S.toolNodes.get(m.call_id) || S.toolNodes.get(String(m.call_id));
  if (!card) return;
  card.querySelector(".tool-state").classList.add("done");
  card.querySelector(".tool-body pre").textContent = m.output || m.display || "(无输出)";
  if (nearBottom()) scrollDown();
}
function appendSubagent(name, summary) {
  stream.appendChild(el("div", "subagent", `↳ <b>${esc(name)}</b> ${esc(summary)}`));
  scrollDown();
}

/* ── permission ──────────────────────────────────────────── */
function onAsk(m) {
  S.ask = { id: m.id, suggested: m.suggested_pattern || "" };
  $("perm-tool").textContent = m.tool || "tool";
  $("perm-input").textContent = m.input || "";
  $("perm").hidden = false; scrollDown();
}
function answer(decision) {
  if (!S.ask) return;
  const payload = { type: "permission_reply", id: S.ask.id, decision };
  if (decision === "allow_always") payload.pattern = S.ask.suggested;
  send(payload);
  S.ask = null; $("perm").hidden = true;
}

/* ── stats ───────────────────────────────────────────────── */
function onUsage(m) {
  if (m.totals) {
    $("st-in").textContent = fmtTok(m.totals.input_tokens);
    $("st-out").textContent = fmtTok(m.totals.output_tokens);
    $("st-cache-r").textContent = fmtTok(m.totals.cached_input_tokens);
  }
  updateCtx(m.estimated_context_tokens);
}
function onDone(m) {
  setBusy(false);
  if (m.totals) {
    $("st-in").textContent = fmtTok(m.totals.input_tokens);
    $("st-out").textContent = fmtTok(m.totals.output_tokens);
    $("st-cache-r").textContent = fmtTok(m.totals.cached_input_tokens);
  }
  $("st-cost").textContent = fmtCost(m.cost);
  updateCtx(m.estimated_context_tokens);
  if (!S.live && m.response) {
    const n = el("div", "msg assistant");
    n.appendChild(el("div", "md", md(m.response)));
    stream.appendChild(n); scrollDown(true);
  }
}
function onTurnEnd() {
  setBusy(false);
  finalizeLive();
  if (S.loop && S.loop.active) return;   // server drives the next iteration
  drainQueue();
}
function updateStats(s) {
  if (s.totals) {
    $("st-in").textContent = fmtTok(s.totals.input_tokens);
    $("st-out").textContent = fmtTok(s.totals.output_tokens);
    $("st-cache-r").textContent = fmtTok(s.totals.cached_input_tokens);
  }
  $("st-cost").textContent = fmtCost(s.cost);
  $("kv-id").textContent = (s.id || "").slice(0, 8);
  $("kv-model").textContent = `${s.provider}/${s.model}`;
  $("tb-session").textContent = s.name || "新对话";
  if (s.working_dir) { $("cwd-text").textContent = s.working_dir; $("btn-cwd").title = s.working_dir; }
  updateCtx(s.estimated_context_tokens);
}
function updateCtx(used) {
  const win = (S.session && S.session.context_window) || 0;
  used = used || 0;
  $("ctx-used").textContent = fmtTok(used);
  $("ctx-window").textContent = win ? fmtTok(win) : "—";
  const pct = win ? Math.min(100, Math.round((used / win) * 100)) : 0;
  $("ctx-pct").textContent = pct + "%";
  $("ctx-fill").style.width = pct + "%";
  $("ctx-fill").style.background = pct > 85 ? "var(--err)" : pct > 65 ? "var(--warn)" : "var(--accent)";
}

/* ── sidebar: sessions ───────────────────────────────────── */
function renderSessions(list) {
  const box = $("sessions"); box.innerHTML = "";
  if (!list.length) { box.appendChild(el("div", "sess-meta", "还没有会话")); return; }
  list.forEach((s) => {
    const b = el("button", "sess");
    if (S.session && S.session.id === s.id) b.classList.add("on");
    const name = s.name || (s.last || "").slice(0, 28) || "未命名";
    b.appendChild(el("span", "sess-name", esc(name)));
    b.appendChild(el("span", "sess-meta", `${s.messages} 条 · ${(s.updated_at || "").slice(5, 16).replace("T", " ")}`));
    const del = el("button", "sess-del", "✕");
    del.onclick = (e) => {
      e.stopPropagation();
      if (confirm(`删除会话「${name}」？`)) send({ type: "delete_session", id: s.id });
    };
    b.appendChild(del);
    b.onclick = () => { send({ type: "resume", id: s.id }); $("sidebar").classList.remove("open"); };
    box.appendChild(b);
  });
}

/* ── sidebar: file tree ──────────────────────────────────── */
function loadTree(path) { send({ type: "list_dir", path }); }

function onDirList(m) {
  const box = $("tree");
  if (m.path === "." || m.path === "") { box.innerHTML = ""; }
  const target = m.path === "." || m.path === "" ? null : box.querySelector(`[data-dir="${CSS.escape(m.path)}"] .tree-kids`);
  const holder = target || box;
  if (!target && !m.entries.length) { holder.appendChild(el("div", "sess-meta", "空目录")); return; }
  const kids = target || el("div", "tree-kids-root");
  kids.innerHTML = "";
  m.entries.forEach((e) => {
    const row = el("div", "tree-row");
    const label = el("button", "tree-item" + (e.dir ? " dir" : ""), `${e.dir ? "▸" : "·"} ${esc(e.name)}`);
    row.appendChild(label);
    if (e.dir) {
      label.dataset.dir = e.path;
      label.onclick = () => {
        const kids = row.querySelector(".tree-kids") || row.appendChild(el("div", "tree-kids"));
        const open = kids.dataset.open === "1";
        if (open) { kids.innerHTML = ""; kids.dataset.open = "0"; label.textContent = `▸ ${e.name}`; }
        else { kids.dataset.open = "1"; label.textContent = `▾ ${e.name}`; loadTree(e.path); }
      };
    } else {
      label.onclick = () => { insertMention(e.path); };
    }
    kids.appendChild(row);
  });
  if (!target) box.appendChild(kids);
}

/* ── @ mention ───────────────────────────────────────────── */
function currentMentionToken() {
  const pos = input.selectionStart;
  const before = input.value.slice(0, pos);
  const m = before.match(/(?:^|\s)@([^\s@]*)$/);
  return m ? { start: before.length - m[1].length - 1, query: m[1] } : null;
}
/* ── / slash commands ────────────────────────────────────── */
const COMMANDS = [
  { cmd: "/loop", args: "<任务> [max=N]", desc: "长任务循环,每轮自动接续" },
  { cmd: "/loop stop", args: "", desc: "停止循环" },
  { cmd: "/btw", args: "<问题>", desc: "并行旁问,不影响当前会话" },
  { cmd: "/compact", args: "", desc: "压缩上下文(保留最近几轮)" },
  { cmd: "/undo", args: "", desc: "撤销上一轮对话" },
  { cmd: "/retry", args: "", desc: "重试上一轮" },
  { cmd: "/export", args: "", desc: "导出 Markdown 转写" },
  { cmd: "/wt", args: "<分支名>", desc: "新建并切到 worktree" },
  { cmd: "/wt-merge", args: "[目标分支]", desc: "合并当前 worktree 并清理" },
  { cmd: "/wt-exit", args: "", desc: "退出 worktree 回到主仓库" },
  { cmd: "/prompt", args: "[名称]", desc: "切换提示词模式" },
  { cmd: "/mode", args: "[模式]", desc: "切换权限模式" },
  { cmd: "/models", args: "", desc: "打开模型与 Provider 管理" },
  { cmd: "/new", args: "", desc: "新建会话" },
  { cmd: "/help", args: "", desc: "查看全部命令" },
];
const NO_ARG = new Set(["/compact", "/undo", "/retry", "/export", "/models", "/new",
                        "/help", "/wt-merge", "/wt-exit", "/loop stop"]);

function currentSlashToken() {
  const pos = input.selectionStart;
  const before = input.value.slice(0, pos);
  if (!before.startsWith("/")) return null;
  if (/\s/.test(before)) return null;          // already past the command word
  return { start: 0, query: before };
}

function onInputChange() {
  resize();
  const sTok = currentSlashToken();
  if (sTok) {
    const q = sTok.query.toLowerCase();
    const items = COMMANDS.filter(
      (c) => c.cmd.toLowerCase().startsWith(q) ||
             c.cmd.toLowerCase().includes(q.slice(1)) ||
             (q === "/" ));
    renderPopup(items, "slash");
    return;
  }
  const tok = currentMentionToken();
  if (tok) {
    send({ type: "complete_path", prefix: tok.query });
  } else {
    closePopup();
  }
}

function closePopup() {
  $("mention").hidden = true;
  S.mention.items = []; S.mention.idx = -1; S.mention.kind = null;
}

function renderPopup(items, kind) {
  S.mention.kind = kind;
  S.mention.items = items.slice(0, 12);
  S.mention.idx = S.mention.items.length ? 0 : -1;
  const box = $("mention");
  if (!S.mention.items.length) { box.hidden = true; return; }
  box.innerHTML = "";
  S.mention.items.forEach((it, i) => {
    let label, hint = "";
    if (kind === "slash") {
      label = esc(it.cmd) + (it.args ? " " + esc(it.args) : "");
      hint = esc(it.desc);
    } else {
      label = esc(it);
    }
    const b = el("button", "mi" + (i === S.mention.idx ? " on" : ""),
      `<span class="mi-label">${label}</span>${hint ? `<code>${hint}</code>` : ""}`);
    b.onclick = () => pickItem(i);
    box.appendChild(b);
  });
  box.hidden = false;
}

function pickItem(i) {
  if (S.mention.kind === "slash") return pickSlash(i);
  return pickMention(i);
}

function pickSlash(i) {
  const item = S.mention.items[i];
  if (!item) return;
  input.value = item.cmd + (item.args && !NO_ARG.has(item.cmd) ? " " : "");
  const caret = input.value.length;
  input.setSelectionRange(caret, caret);
  closePopup();
  resize();
  if (NO_ARG.has(item.cmd)) { const t = input.value; input.value = ""; resize(); dispatch(t); }
  else input.focus();
}
function onPathMatches(items) {
  if (S.mention.kind === "slash") return;   // 斜杠菜单优先
  renderPopup(items, "path");
}
function pickMention(i) {
  const item = S.mention.items[i];
  if (!item) return;
  const tok = currentMentionToken();
  const pos = input.selectionStart;
  if (tok) {
    const next = input.value.slice(0, tok.start) + "@" + item + " " + input.value.slice(pos);
    input.value = next;
    const caret = tok.start + item.length + 2;
    input.setSelectionRange(caret, caret);
  }
  $("mention").hidden = true;
  S.mention.items = []; S.mention.idx = -1;
  input.focus();
}
function insertMention(path) {
  input.value += (input.value && !input.value.endsWith(" ") ? " " : "") + "@" + path + " ";
  resize(); input.focus();
}

/* ── attachments ─────────────────────────────────────────── */
function renderAttached(files) {
  S.attached = files;
  const box = $("attach");
  box.innerHTML = "";
  if (!files.length) { box.hidden = true; return; }
  files.forEach((f) => {
    const chip = el("span", "chip");
    chip.appendChild(el("span", "chip-name", esc(f.split("/").pop())));
    chip.title = f;
    const x = el("button", "chip-x", "✕");
    x.onclick = () => send({ type: "remove_file", path: f });
    chip.appendChild(x);
    box.appendChild(chip);
  });
  box.hidden = false;
}

async function uploadFiles(fileList) {
  for (const f of fileList) {
    try {
      const r = await fetch("/upload", {
        method: "POST",
        headers: { "x-filename": encodeURIComponent(f.name) },
        body: f,
      });
      const j = await r.json();
      if (j.ok) send({ type: "add_file", path: j.path });
      else sysmsg(`上传失败 ${f.name}: ${j.error || "unknown"}`, "err");
    } catch (e) {
      sysmsg(`上传失败 ${f.name}: ${e}`, "err");
    }
  }
}

/* ── queue ───────────────────────────────────────────────── */
function renderQueue() {
  const q = $("queue");
  q.innerHTML = "";
  if (!S.queue.length) { q.hidden = true; return; }
  q.hidden = false;
  S.queue.forEach((t, i) => {
    const row = el("div", "qitem");
    row.appendChild(el("span", "qidx", String(i + 1)));
    row.appendChild(el("span", "qtext", esc(t.slice(0, 120))));
    const x = el("button", "icon ghost tiny", "✕");
    x.onclick = () => { S.queue.splice(i, 1); renderQueue(); };
    row.appendChild(x);
    q.appendChild(row);
  });
}
function drainQueue() {
  if (S.busy || !S.queue.length || !S.connected) return;
  const text = S.queue.shift();
  renderQueue();
  dispatch(text);
}

/* ── slash routing ───────────────────────────────────────── */
function slash(text) {
  const t = text.trim();
  if (!t.startsWith("/")) return false;
  const [cmd, ...rest] = t.split(/\s+/);
  const arg = t.slice(cmd.length).trim();
  switch (cmd) {
    case "/compact": send({ type: "compact" }); sysmsg("正在压缩上下文…"); return true;
    case "/undo": send({ type: "undo_turn" }); return true;
    case "/retry": send({ type: "retry_turn" }); return true;
    case "/export": send({ type: "export_session" }); return true;
    case "/btw":
      if (!arg) { sysmsg("用法: /btw <问题>", "err"); return true; }
      startBtw(arg); return true;
    case "/loop":
      if (arg === "stop" || arg === "off") { send({ type: "loop_stop" }); return true; }
      if (!arg) { sysmsg("用法: /loop <任务> [max=10]  |  /loop stop", "err"); return true; }
      {
        const mMax = arg.match(/\bmax=(\d+)\b/);
        const max = mMax ? Number(mMax[1]) : 10;
        const task = arg.replace(/\bmax=\d+\b/, "").trim();
        send({ type: "loop_start", prompt: task, max });
        sysmsg(`循环启动(最多 ${max} 轮): ${task}`);
      }
      return true;
    case "/wt":
      if (!arg) { sysmsg("用法: /wt <分支名>", "err"); return true; }
      send({ type: "worktree_new", name: arg }); return true;
    case "/wt-merge": send({ type: "worktree_merge", target: arg || null }); return true;
    case "/wt-exit": send({ type: "worktree_exit" }); return true;
    case "/models": openConfig(); return true;
    case "/clear": send({ type: "new_session" }); return true;
    case "/new": send({ type: "new_session" }); return true;
    case "/prompt":
      if (!arg) {
        sysmsg("可用提示词: " + S.prompts.map(promptLabel).join("  |  "));
        return true;
      }
      send({ type: "set_prompt", name: arg === "default" ? null : arg }); return true;
    case "/mode":
      if (!arg) { sysmsg("可用模式: " + S.modes.join(", ")); return true; }
      send({ type: "set_mode", mode: arg }); return true;
    case "/help":
      sysmsg("命令: /loop /btw /compact /undo /retry /export /wt /wt-merge /wt-exit /prompt /mode /models /new");
      return true;
    default:
      sysmsg(`未知命令 ${cmd}，试试 /help`, "err");
      return true;
  }
}

/* ── btw ─────────────────────────────────────────────────── */
function startBtw(q) {
  S.btwCard = el("div", "btw");
  S.btwCard.appendChild(el("div", "btw-head", `旁问 · ${esc(q)}`));
  S.btwCard.appendChild(el("div", "btw-body md", '<span class="cursor"></span>'));
  stream.appendChild(S.btwCard);
  S.btwBuf = "";
  send({ type: "btw", text: q });
  scrollDown(true);
}
function onBtwToken(t) {
  if (!S.btwCard) return;
  S.btwBuf += t;
  S.btwCard.querySelector(".btw-body").innerHTML = md(S.btwBuf) + '<span class="cursor"></span>';
  if (nearBottom()) scrollDown();
}
function onBtwTool(name) {
  if (!S.btwCard) return;
  S.btwCard.appendChild(el("div", "tool-sub", `只读工具: ${esc(name)}`));
}
function onBtwDone(m) {
  if (!S.btwCard) return;
  const body = S.btwCard.querySelector(".btw-body");
  if (m.error) body.innerHTML = `<span style="color:var(--err)">${esc(m.error)}</span>`;
  else body.innerHTML = md(m.text || S.btwBuf);
}
function onBtwEnd() { if (S.btwCard) S.btwCard.classList.add("done"); }
function resetBtw() { S.btwCard = null; S.btwBuf = ""; }

/* ── loop ────────────────────────────────────────────────── */
function onLoopStatus(m) {
  S.loop = m.active ? m : null;
  const h = $("hints");
  h.innerHTML = "";
  if (m.active) {
    const chip = el("span", "h loop", `循环 ${m.iteration}/${m.max}`);
    const stop = el("button", "chip-btn tiny", "停止");
    stop.onclick = () => send({ type: "loop_stop" });
    h.appendChild(chip); h.appendChild(stop);
  }
  $("foot-hints").textContent = m.active
    ? `长任务循环进行中：${m.prompt || ""}`
    : "";
}

/* ── config modal ────────────────────────────────────────── */
function openConfig() { send({ type: "config_get" }); $("modal").hidden = false; }
function renderConfig(m) {
  $("cfg-file").textContent = m.config_file || "—";
  const box = $("cfg-models"); box.innerHTML = "";
  (m.models || []).forEach((x) => {
    const row = el("div", "cfg-row");
    row.appendChild(el("code", null, `${esc(x.name)} → ${esc(x.provider)}/${esc(x.model)}`));
    row.appendChild(el("span", "tag " + (x.source === "ui" ? "ui" : ""), x.source));
    if (x.source === "ui") {
      const del = el("button", "icon ghost tiny", "✕");
      del.onclick = () => send({ type: "remove_model", name: x.name });
      row.appendChild(del);
    }
    box.appendChild(row);
  });
  const pbox = $("cfg-providers"); pbox.innerHTML = "";
  (m.providers || []).forEach((p) => {
    const row = el("div", "cfg-row");
    row.appendChild(el("code", null, p.name));
    row.appendChild(el("span", "tag", p.kind + (p.base_url ? ` · ${esc(p.base_url)}` : "")));
    pbox.appendChild(row);
  });
}

/* ── controls ────────────────────────────────────────────── */
function setBusy(b) {
  S.busy = b;
  $("btn-send").disabled = b;
  $("btn-stop").hidden = !b;
  $("composer").style.opacity = b ? ".85" : "1";
}
function dispatch(text) {
  if (slash(text)) return;
  finalizeLive(); resetBtw();
  appendUser(text);
  send({ type: "prompt", text });
  setBusy(true);
  scrollDown(true);
}
function submit() {
  const text = input.value.trim();
  if (!text) return;
  input.value = ""; resize();
  $("mention").hidden = true;
  S.history.push(text); S.histIdx = -1;
  if (S.busy) {
    if (text.startsWith("/")) { sysmsg("运行中不支持该命令，等本轮结束", "err"); return; }
    S.queue.push(text); renderQueue(); return;
  }
  dispatch(text);
}
function resize() {
  input.style.height = "auto";
  input.style.height = Math.min(200, input.scrollHeight) + "px";
}

input.addEventListener("keydown", (e) => {
  const open = !$("mention").hidden && S.mention.items.length > 0;
  if (open && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
    e.preventDefault();
    const n = S.mention.items.length;
    S.mention.idx = (S.mention.idx + (e.key === "ArrowDown" ? 1 : n - 1)) % n;
    $("mention").querySelectorAll(".mi").forEach((b, i) => b.classList.toggle("on", i === S.mention.idx));
    return;
  }
  if (open && (e.key === "Tab" || (e.key === "Enter" && !e.shiftKey))) {
    e.preventDefault(); pickItem(S.mention.idx); return;
  }
  if (open && e.key === "Escape") { e.preventDefault(); closePopup(); return; }
  if (e.key === "Enter" && !e.shiftKey && !e.isComposing) { e.preventDefault(); submit(); return; }
  if (e.key === "ArrowUp" && !input.value && S.history.length) {
    e.preventDefault();
    S.histIdx = S.histIdx < 0 ? S.history.length - 1 : Math.max(0, S.histIdx - 1);
    input.value = S.history[S.histIdx]; resize(); return;
  }
  if (e.key === "ArrowDown" && S.histIdx >= 0) {
    e.preventDefault(); S.histIdx++;
    if (S.histIdx >= S.history.length) { S.histIdx = -1; input.value = ""; }
    else input.value = S.history[S.histIdx];
    resize();
  }
});
input.addEventListener("input", onInputChange);

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    if (S.busy) send({ type: "cancel" });
    $("menu").hidden = true;
    $("modal").hidden = true;
    $("mention").hidden = true;
  }
});

$("btn-attach").onclick = () => $("file-input").click();
$("file-input").onchange = (e) => { uploadFiles(e.target.files); e.target.value = ""; };

["dragover", "dragenter"].forEach((ev) =>
  $("composer").addEventListener(ev, (e) => {
    e.preventDefault(); $("composer").classList.add("drop");
  })
);
["dragleave", "drop"].forEach((ev) =>
  $("composer").addEventListener(ev, (e) => {
    e.preventDefault(); $("composer").classList.remove("drop");
  })
);
$("composer").addEventListener("drop", (e) => {
  if (e.dataTransfer && e.dataTransfer.files.length) uploadFiles(e.dataTransfer.files);
});

$("btn-send").onclick = submit;
$("btn-stop").onclick = () => send({ type: "cancel" });
$("btn-new").onclick = () => { send({ type: "new_session" }); $("sidebar").classList.remove("open"); };
$("btn-sidebar").onclick = () => $("sidebar").classList.toggle("open");
$("btn-panel").onclick = () => { const p = $("panel"); p.hidden = !p.hidden; };
$("panel-close").onclick = () => ($("panel").hidden = true);
$("modal-close").onclick = () => ($("modal").hidden = true);
$("modal").addEventListener("click", (e) => { if (e.target.id === "modal") $("modal").hidden = true; });

$("btn-rename").onclick = () => {
  const cur = (S.session && S.session.name) || "";
  const name = prompt("会话名称", cur);
  if (name != null) send({ type: "rename_session", name });
};
$("btn-cwd").onclick = () => {
  const next = prompt("切换工作目录（绝对路径或 ~ 开头）", S.cwd || "");
  if (next) { send({ type: "set_cwd", path: next }); loadTree("."); }
};
$("sel-prompt").onchange = (e) => send({ type: "set_prompt", name: e.target.value || null });
$("sel-mode").onchange = (e) => send({ type: "set_mode", mode: e.target.value });
$("sel-model").onchange = (e) => send({ type: "switch_model", name: e.target.value });
$("btn-reason").onclick = () => send({ type: "set_reasoning", enabled: !S.reasoning });

document.querySelectorAll(".perm-actions .btn").forEach((b) => {
  b.onclick = () => answer(b.dataset.dec || "allow_once");
});
$("perm-always").dataset.dec = "allow_always";
$("perm-close").onclick = () => answer("deny");

/* menu */
$("btn-menu").onclick = (e) => { e.stopPropagation(); $("menu").hidden = !$("menu").hidden; };
document.addEventListener("click", (e) => {
  if (!$("menu").hidden && !$("menu").contains(e.target) && e.target.id !== "btn-menu") $("menu").hidden = true;
});
$("menu").querySelectorAll(".mi").forEach((b) => {
  b.onclick = () => {
    const act = b.dataset.act;
    $("menu").hidden = true;
    if (act === "compact") send({ type: "compact" });
    else if (act === "undo") send({ type: "undo_turn" });
    else if (act === "retry") send({ type: "retry_turn" });
    else if (act === "export") send({ type: "export_session" });
    else if (act === "settings") openConfig();
    else if (act === "btw") {
      const q = prompt("旁问（不影响当前会话）");
      if (q) startBtw(q);
    } else if (act === "loop") {
      const q = prompt("循环任务（最多 10 轮，可输入 max=N 指定）");
      if (q) dispatch(`/loop ${q}`);
    } else if (act === "wt-new") {
      const n = prompt("新 worktree 分支名");
      if (n) send({ type: "worktree_new", name: n });
    } else if (act === "wt-merge") send({ type: "worktree_merge", target: null });
    else if (act === "wt-exit") send({ type: "worktree_exit" });
  };
});

$("btn-add-model").onclick = () => {
  const name = $("m-name").value.trim();
  const provider = $("m-provider").value.trim();
  const model = $("m-model").value.trim();
  if (!name || !provider || !model) return;
  send({ type: "add_model", name, provider, model });
  $("m-name").value = $("m-provider").value = $("m-model").value = "";
};

/* sidebar tabs */
document.querySelectorAll(".side-tab").forEach((t) => {
  t.onclick = () => {
    document.querySelectorAll(".side-tab").forEach((x) => x.classList.toggle("on", x === t));
    const files = t.dataset.tab === "files";
    $("pane-sessions").hidden = files;
    $("pane-files").hidden = !files;
    if (files && !$("tree").childElementCount) loadTree(".");
  };
});

/* ── theme ───────────────────────────────────────────────── */
function applyTheme(t) {
  document.documentElement.dataset.theme = t;
  localStorage.setItem("zs-theme", t);
}
$("btn-theme").onclick = () =>
  applyTheme(document.documentElement.dataset.theme === "dark" ? "light" : "dark");
applyTheme(localStorage.getItem("zs-theme") ||
  (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"));

/* ── export download ─────────────────────────────────────── */
function download(filename, content) {
  const blob = new Blob([content], { type: "text/markdown;charset=utf-8" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

/* ── render model select ─────────────────────────────────── */
function renderModels() {
  const sm = $("sel-model");
  sm.innerHTML = "";
  S.models.forEach((m) => {
    const label = m.name === m.model ? m.model : `${m.name} · ${m.model}`;
    sm.appendChild(new Option(label, m.name));
  });
  if (S.session) {
    const hit = S.models.find((m) => m.model === S.session.model && m.provider === S.session.provider);
    if (hit) sm.value = hit.name;
  }
}

/* ── go ──────────────────────────────────────────────────── */
renderQueue();
renderSessions([]);
connect();
input.focus();
