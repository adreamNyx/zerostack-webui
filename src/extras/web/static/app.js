/* zerostack web ui — 前端逻辑
 * 职责：WS 连接管理、会话渲染（含恢复历史）、流式 token、工具卡片、
 * 权限弹窗、统计面板。与后端的 JSON 协议见 extras/web/mod.rs 头注释。
 */
"use strict";

const $ = (id) => document.getElementById(id);
const stream = $("stream");

/* ── 状态 ── */
let ws = null;
let busy = false;
let currentSessionId = null;
let streamingEl = null;      // 正在流式输出的 assistant 气泡
let reasoningEl = null;      // 正在流式输出的思考块
const toolCards = new Map(); // call_id → 卡片元素
const S = {                  // 统计面板数据源
  totals: { input_tokens: 0, output_tokens: 0, cached_input_tokens: 0, cache_creation_input_tokens: 0 },
  cost: 0, msgCount: 0, id: "—", name: "", model: "—", provider: "—",
  working_dir: "", context_window: 0, estimated_context_tokens: 0,
};

/* ── markdown ── */
function md(text) {
  try {
    if (window.marked) {
      const html = marked.parse(text, { breaks: true, mangle: false, headerIds: false });
      // 防 XSS：模型输出可能包含任意 HTML/脚本，消毒后再插入 DOM
      if (window.DOMPurify) return DOMPurify.sanitize(html);
      return html;
    }
  } catch (e) { /* fallback below */ }
  return "<p>" + text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/\n/g, "<br>") + "</p>";
}
function esc(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function fmtTok(n) {
  if (n >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "k";
  return String(n);
}
function fmtCost(c) { return "¥" + Number(c || 0).toFixed(4); }
function scrollDown(force) {
  const near = stream.scrollHeight - stream.scrollTop - stream.clientHeight < 160;
  if (near || force) stream.scrollTop = stream.scrollHeight;
}

/* ── 消息元素构造 ── */
function addMsg(cls, whoText) {
  const div = document.createElement("div");
  div.className = "msg " + cls;
  div.innerHTML = `<div class="who">${esc(whoText)}</div><div class="body"></div>`;
  stream.appendChild(div);
  return div.querySelector(".body");
}
function addSys(text, cls) {
  const d = document.createElement("div");
  d.className = "sysline" + (cls ? " " + cls : "");
  d.textContent = text;
  stream.appendChild(d);
  scrollDown();
  return d;
}
function addToolCard(callId, name, summary) {
  const card = document.createElement("div");
  card.className = "tool running";
  card.innerHTML = `
    <div class="tool-h">
      <span class="t-ico">◈</span>
      <span class="t-name">${esc(name)}</span>
      <span class="t-sum">${esc(summary || "")}</span>
      <span class="t-st">运行中</span>
    </div>
    <div class="tool-b">
      <div class="sec"><div class="sec-k">ARGS</div><pre class="t-args"></pre></div>
      <div class="sec"><div class="sec-k">OUTPUT</div><pre class="t-out">(等待结果…)</pre></div>
    </div>`;
  card.querySelector(".tool-h").addEventListener("click", () => card.classList.toggle("open"));
  stream.appendChild(card);
  if (callId !== null && callId !== undefined) toolCards.set(String(callId), card);
  scrollDown();
  return card;
}
function addPermAsk(ask) {
  const card = document.createElement("div");
  card.className = "perm";
  card.innerHTML = `
    <div class="perm-h">
      <div class="p-t">⛔ 权限确认</div>
      <div class="p-tool">${esc(ask.tool)}</div>
      <pre>${esc(ask.input)}</pre>
      <div class="p-pat">永久允许模式：${esc(ask.suggested_pattern || "—")}</div>
    </div>
    <div class="perm-b">
      <button class="allow">✓ 允许一次</button>
      <button class="always">⟳ 永久允许</button>
      <button class="deny">✕ 拒绝</button>
    </div>`;
  const answer = (decision, pattern) => {
    card.classList.add("answered");
    ws.send(JSON.stringify({ type: "permission_reply", id: ask.id, decision, pattern }));
  };
  card.querySelector(".allow").onclick = () => answer("allow_once");
  card.querySelector(".always").onclick = () => answer("allow_always", ask.suggested_pattern);
  card.querySelector(".deny").onclick = () => answer("deny");
  stream.appendChild(card);
  scrollDown(true);
}

/* ── 流式输出 ── */
function ensureStreaming() {
  if (!streamingEl) {
    const body = addMsg("agent", "ZEROSTACK");
    body.innerHTML = '<span class="md-body"></span><span class="cursor"></span>';
    streamingEl = body.querySelector(".md-body");
    streamingEl._raw = "";
  }
  return streamingEl;
}
function endStreaming() {
  if (streamingEl) {
    // innerHTML 赋值同时移除了流式光标
    streamingEl.innerHTML = md(streamingEl._raw);
    streamingEl = null;
  }
  reasoningEl = null;
}

/* ── 模型切换 ── */
function renderModelSelect(models) {
  S.models = models || [];
  const sel = $("model-select");
  if (!sel) return;
  sel.innerHTML = "";
  for (const m of S.models) {
    const opt = document.createElement("option");
    opt.value = m.name;
    opt.textContent = `${m.name} · ${m.model}`;
    sel.appendChild(opt);
  }
  syncModelSelect();
}
function syncModelSelect() {
  const sel = $("model-select");
  if (!sel) return;
  sel.title = `${S.provider || "—"} / ${S.model || "—"}`;
  const cur = (S.models || []).find(
    m => m.provider === S.provider && m.model === S.model
  );
  // 当前模型不在快捷列表里时，临时插入一项以便正确显示
  if (!cur && S.model) {
    let tmp = sel.querySelector("option[data-current]");
    if (!tmp) {
      tmp = document.createElement("option");
      tmp.dataset.current = "1";
      sel.appendChild(tmp);
    }
    tmp.value = "__current__";
    tmp.textContent = `当前 · ${S.model}`;
    sel.value = "__current__";
  } else if (cur) {
    sel.querySelector("option[data-current]")?.remove();
    sel.value = cur.name;
  }
}
$("model-select").addEventListener("change", e => {
  const name = e.target.value;
  if (name && name !== "__current__") {
    addSys(`◆ 正在切换模型 → ${name} …`);
    send({ type: "switch_model", name });
  }
});

/* ── 统计面板 ── */
function updateStats() {
  $("st-in").textContent = fmtTok(S.totals.input_tokens);
  $("st-out").textContent = fmtTok(S.totals.output_tokens);
  $("st-cache-r").textContent = fmtTok(S.totals.cached_input_tokens);
  $("st-cache-w").textContent = fmtTok(S.totals.cache_creation_input_tokens);
  $("st-cost").textContent = fmtCost(S.cost);
  $("st-msgs").textContent = S.msgCount;
  $("kv-id").textContent = String(S.id).slice(0, 8);
  $("kv-model").textContent = S.model;
  $("kv-provider").textContent = S.provider;
  $("side-cwd").textContent = S.working_dir || "—";
  $("tb-session").textContent = S.name || String(S.id).slice(0, 8);
  syncModelSelect();
  const used = S.estimated_context_tokens || 0;
  const win = S.context_window || 0;
  $("ctx-used").textContent = fmtTok(used);
  $("ctx-window").textContent = fmtTok(win);
  $("hint-context").textContent = "ctx " + fmtTok(used) + (win ? " / " + fmtTok(win) : "");
  $("hint-cost").textContent = fmtCost(S.cost);
  if (win) {
    const pct = Math.min(100, (used / win) * 100);
    $("ctx-pct").textContent = pct.toFixed(1) + "%";
    $("ctx-fill").style.width = pct + "%";
  }
}

/* ── 会话历史渲染 ── */
function renderHistory(messages) {
  stream.innerHTML = "";
  toolCards.clear();
  streamingEl = null;
  for (const m of messages) {
    const role = m.role;
    if (role === "user") {
      addMsg("user", "你").textContent = m.content;
    } else if (role === "assistant") {
      addMsg("agent", "ZEROSTACK").innerHTML = md(m.content);
    } else if (role === "tool_call" && m.tool) {
      const card = addToolCard(m.tool.id, m.tool.name, m.content);
      card.querySelector(".t-args").textContent = JSON.stringify(m.tool.args, null, 2);
      card.classList.remove("running");
      card.querySelector(".t-st").textContent = "";
    } else if (role === "tool_result" && m.tool) {
      const card = toolCards.get(String(m.tool.call_id));
      if (card) {
        card.querySelector(".t-out").textContent = m.content;
        card.querySelector(".t-st").textContent = m.tool.truncated ? "已截断" : "完成";
        card.querySelector(".t-st").classList.add("done");
      }
    } else if (role === "subagent_tool_call" && m.tool) {
      const d = document.createElement("div");
      d.className = "subagent";
      d.innerHTML = `<b>[子代理]</b> ${esc(m.tool.name)} — ${esc(m.content)}`;
      stream.appendChild(d);
    }
  }
  scrollDown(true);
}

/* ── WS ── */
function connect() {
  const proto = location.protocol === "https:" ? "wss://" : "ws://";
  ws = new WebSocket(proto + location.host + "/ws");
  ws.onopen = () => {
    setStatus("on", "已连接");
    send({ type: "session_list" });
  };
  ws.onclose = () => {
    setStatus("off", "已断开");
    setTimeout(connect, 2000);
  };
  ws.onmessage = (e) => {
    let v;
    try { v = JSON.parse(e.data); } catch { return; }
    handle(v);
  };
}
function send(obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function setStatus(cls, kvText) {
  const dot = $("tb-status");
  dot.className = "tb-dot " + cls;
  const kv = $("kv-ws");
  kv.textContent = kvText;
  kv.className = cls === "on" ? "ok" : "err";
}
function setBusy(b) {
  busy = b;
  $("btn-send").disabled = b;
  $("btn-stop").classList.toggle("hidden", !b);
  $("btn-send").classList.toggle("hidden", b);
  const run = $("kv-run");
  run.textContent = b ? "运行中" : "空闲";
  run.className = b ? "busy" : "ok";
  if (b) $("tb-status").className = "tb-dot busy";
  else if (ws && ws.readyState === 1) $("tb-status").className = "tb-dot on";
}

/* ── 服务端消息处理 ── */
function handle(v) {
  switch (v.type) {
    case "session_state": {
      endStreaming();
      currentSessionId = v.session.id;
      Object.assign(S, v.session);
      S.msgCount = v.messages.length;
      renderModelSelect(v.models);
      renderHistory(v.messages);
      updateStats();
      refreshList();
      break;
    }
    case "session_list": {
      const list = $("session-list");
      list.innerHTML = "";
      for (const s of v.sessions) {
        const d = document.createElement("div");
        d.className = "sess" + (s.id === currentSessionId ? " active" : "");
        const name = s.name || s.last || "(空会话)";
        d.innerHTML = `<div class="s-name">${esc(name)}</div>
          <div class="s-meta">${esc(s.id.slice(0, 8))} · ${esc(s.model)} · ${s.messages}条</div>`;
        d.onclick = () => send({ type: "resume", id: s.id });
        list.appendChild(d);
      }
      break;
    }
    case "token": {
      const el = ensureStreaming();
      el._raw += v.text;
      el.innerHTML = md(el._raw) + '<span class="cursor"></span>';
      scrollDown();
      break;
    }
    case "reasoning": {
      if (!reasoningEl) {
        reasoningEl = document.createElement("div");
        reasoningEl.className = "reasoning";
        stream.appendChild(reasoningEl);
      }
      reasoningEl.textContent += v.text;
      reasoningEl.scrollTop = reasoningEl.scrollHeight;
      scrollDown();
      break;
    }
    case "tool_call": {
      reasoningEl = null;
      endStreaming();
      const card = addToolCard(v.call_id, v.name, v.summary);
      card.querySelector(".t-args").textContent = JSON.stringify(v.args, null, 2);
      break;
    }
    case "tool_result": {
      const card = toolCards.get(String(v.call_id));
      if (card) {
        card.classList.remove("running");
        card.classList.add("open");
        card.querySelector(".t-out").textContent = v.output;
        card.querySelector(".t-st").textContent = "完成";
        card.querySelector(".t-st").classList.add("done");
      }
      scrollDown();
      break;
    }
    case "subagent": {
      const d = document.createElement("div");
      d.className = "subagent";
      d.innerHTML = `<b>[子代理]</b> ${esc(v.name)} — ${esc(v.summary)}`;
      stream.appendChild(d);
      scrollDown();
      break;
    }
    case "permission_ask": {
      addPermAsk(v);
      break;
    }
    case "retrying": {
      addSys(`⟳ 请求失败，重试中 (${v.attempt}/${v.max})`, "warn");
      break;
    }
    case "usage": {
      if (v.totals) {
        S.totals = v.totals;
        S.estimated_context_tokens = v.estimated_context_tokens;
        updateStats();
      }
      break;
    }
    case "done": {
      endStreaming();
      addSys(`◆ 完成 · 输入 ${fmtTok(v.usage.input_tokens)} · 输出 ${fmtTok(v.usage.output_tokens)} · ${fmtCost(v.cost)}`);
      if (v.totals) S.totals = v.totals;
      S.cost = v.cost;
      S.estimated_context_tokens = v.estimated_context_tokens;
      S.msgCount = stream.querySelectorAll(".msg").length;
      updateStats();
      setBusy(false);
      break;
    }
    case "turn_end": {
      endStreaming();
      setBusy(false);
      if (v.cancelled) addSys("■ 已停止");
      refreshList();
      break;
    }
    case "error": {
      endStreaming();
      addSys("✕ " + v.message, "err");
      setBusy(false);
      break;
    }
  }
}

function refreshList() { send({ type: "session_list" }); }

/* ── 输入交互 ── */
const input = $("input");
function submit() {
  const text = input.value.trim();
  if (!text || busy) return;
  input.value = "";
  autoGrow();
  addMsg("user", "你").textContent = text;
  setBusy(true);
  send({ type: "prompt", text });
}
input.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); submit(); }
});
function autoGrow() {
  input.style.height = "auto";
  input.style.height = Math.min(180, input.scrollHeight) + "px";
}
input.addEventListener("input", autoGrow);
$("btn-send").onclick = submit;
$("btn-stop").onclick = () => send({ type: "cancel" });
$("btn-new").onclick = () => send({ type: "new_session" });
$("btn-panel").onclick = () => {
  const p = $("panel");
  p.style.display = p.style.display === "none" ? "block" : "none";
};
$("btn-sidebar").onclick = () => $("sidebar").classList.toggle("show");

/* 启动 */
connect();
input.focus();
