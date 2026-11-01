//! Web UI host: an axum + WebSocket server that lets a browser act as the
//! frontend ("前台") for the agent core — a parallel replacement for the TUI,
//! not a proxy through it.
//!
//! Design mirrors `extras/acp` for agent construction and the TUI event loop
//! (`ui::app`) for turn driving: one `tokio::select!` per connection
//! multiplexes inbound WS messages, forwarded `AgentEvent`s of the in-flight
//! turn, and permission `AskRequest`s. Session state is the same JSON the TUI
//! reads/writes, so `zerostack --resume` and the web UI can hand a
//! conversation back and forth (shift change, not live sync).
//!
//! WS message protocol (JSON, one object per frame):
//!
//! client → server:
//!   `{"type":"prompt","text":"..."}`                    run one turn
//!   `{"type":"cancel"}`                                 abort in-flight turn
//!   `{"type":"permission_reply","id":"...","decision":"allow_once|allow_always|deny"}`
//!   `{"type":"new_session"}`                            fresh session
//!   `{"type":"resume","id":"<prefix>"}`                 load a saved session
//!   `{"type":"delete_session","id":"<prefix>"}`         delete a saved session
//!   `{"type":"session_list"}`                           refresh sidebar list
//!   `{"type":"switch_model","name":"<quick-model>"}`    hot-swap provider/model
//!   `{"type":"add_file","path":"rel"}`                  attach a file to the next turn
//!   `{"type":"remove_file","path":"rel"}`               detach it again
//!
//! HTTP: `POST /upload` (raw body + `x-filename` header) stores a file under
//! `<cwd>/.zerostack-uploads/`, so the agent can read it like any other file.
//!
//! server → client:
//!   `session_state`, `session_list`, `token`, `reasoning`, `tool_call`,
//!   `tool_result`, `subagent`, `permission_ask`, `retrying`, `usage`,
//!   `done`, `turn_end`, `error`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::Router;
use compact_str::CompactString;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex as AsyncMutex, RwLock as AsyncRwLock};

use crate::agent::builder::AgentBuild;
use crate::agent::runner::{self, AgentRunner};
use crate::cli::Cli;
use crate::config::Config;
use crate::context::ContextFiles;
use crate::event::AgentEvent;
use crate::permission::ask::{AskReceiver, AskSender, UserDecision};
use crate::permission::checker::PermCheck;
use crate::permission::checker::PermissionChecker;
use crate::permission::SecurityMode;
use crate::provider::{build_agent, AnyClient};
use crate::sandbox::Sandbox;
use crate::session::storage;
use crate::session::{MessageRole, Session};
use crate::ui::state::TurnUsage;

const INDEX_HTML: &str = include_str!("static/index.html");
const APP_JS: &str = include_str!("static/app.js");
const STYLE_CSS: &str = include_str!("static/style.css");

const DEFAULT_PORT: u16 = 18800;

/// Everything one `zerostack --web` server needs. Built by
/// `Startup::dispatch_web` from the same pieces the TUI would get: resolved
/// provider/model, created-or-resumed session, client, permission channel,
/// sandbox.
pub(crate) struct WebArgs {
    pub cli: Cli,
    pub cfg: Config,
    pub context: ContextFiles,
    pub client: AnyClient,
    pub session: Session,
    pub permission: Option<PermCheck>,
    pub ask_tx: Option<AskSender>,
    pub ask_rx: Option<AskReceiver>,
    pub sandbox: Sandbox,
    pub provider: String,
    pub model: String,
}

/// One permission request, proxied through the hub so any connection can
/// render the dialog. `reply` stays with the hub; connections answer by id.
struct PermAsk {
    id: String,
    tool: String,
    input: String,
    suggested: String,
}

struct Decision {
    id: String,
    decision: UserDecision,
}

/// Control-plane messages multiplexed into each connection's main loop.
enum ConnMsg {
    /// A complete JSON object arrived from the browser.
    Inbound(Value),
    /// An `AgentEvent` forwarded from the in-flight turn's runner task.
    Agent(AgentEvent),
    /// The runner's event channel closed without a `Done` (upstream abort).
    AgentClosed,
    /// The browser went away (reader exhausted). Wakes the main loop so it
    /// can break instead of parking forever on `recv()`.
    Disconnected,
}

/// The active LLM routing triple. Held behind a RwLock so the `switch_model`
/// WS message can hot-swap it; the agent is rebuilt from these values at the
/// start of every turn, so a switch takes effect on the next prompt.
struct RuntimeModel {
    client: AnyClient,
    provider: String,
    model: String,
}

/// Process-wide UI preferences: the web counterpart of the TUI's slash state.
struct UiState {
    /// Active system-prompt mode (`/prompt`); `None` = the config default.
    prompt: Option<String>,
    /// Reasoning stream visibility (`/reasoning`).
    reasoning: bool,
    /// Permission mode to restore when the prompt is cleared, so a prompt with
    /// a `%%mode=` directive does not leave the session stuck in that mode.
    pre_prompt_mode: Option<SecurityMode>,
}

/// Fallbacks for sessions created after startup.
struct Defaults {
    provider: String,
    model: String,
    context_window: u64,
}

struct AppState {
    cli: Cli,
    cfg: Config,
    context: ContextFiles,
    runtime: AsyncRwLock<RuntimeModel>,
    ui: AsyncRwLock<UiState>,
    defaults: Defaults,
    permission: Option<PermCheck>,
    ask_tx: Option<AskSender>,
    sandbox: Sandbox,
    /// The session the server was started with: handed to the first connection
    /// that attaches. Later connections start a fresh conversation.
    seed: AsyncMutex<Option<Session>>,
    /// AskRequest proxy broadcast (hub → connections).
    ask_bc: broadcast::Sender<Arc<PermAsk>>,
    /// Connection → hub decisions.
    decisions_tx: mpsc::Sender<Decision>,
    /// Quick models added at runtime from the UI (`(name, provider, model)`),
    /// merged over the config entries and persisted to the config file.
    models_overlay: AsyncRwLock<Vec<(String, String, String)>>,
}

/// One running `/loop`: the web counterpart of `extras::loop::LoopState`.
struct LoopRun {
    prompt: String,
    iteration: u32,
    max: u32,
}

/// Per-connection state: an independent conversation, exactly like one TUI
/// process. Nothing here is shared with other connections.
struct Conn {
    session: AsyncMutex<Session>,
    /// One agent turn at a time on this connection.
    busy: AtomicBool,
    /// Active `/loop`, if any. Driven by the main loop after every turn.
    loop_run: AsyncMutex<Option<LoopRun>>,
    /// Files the user attached to this conversation (cwd-relative). Injected
    /// into the turn's `extra_files`, so their contents reach the prompt.
    attached: AsyncMutex<Vec<String>>,
}

// ── entry point ──────────────────────────────────────────────────────────

pub(crate) async fn run(args: WebArgs) -> anyhow::Result<()> {
    let host = args.cli.web_host.clone();
    let port = args.cli.web_port.unwrap_or(DEFAULT_PORT);

    // Permission hub: the process-wide `ask_rx` gets a single consumer task
    // that re-broadcasts requests to any subscribed connection and routes
    // decisions back to the original oneshot.
    let (ask_bc, _) = broadcast::channel::<Arc<PermAsk>>(32);
    let (dec_tx, mut dec_rx) = mpsc::channel::<Decision>(32);
    if let Some(mut ask_rx) = args.ask_rx {
        let ask_bc = ask_bc.clone();
        tokio::spawn(async move {
            let mut pending: HashMap<String, oneshot::Sender<UserDecision>> = HashMap::new();
            loop {
                tokio::select! {
                    req = ask_rx.recv() => {
                        let Some(req) = req else { break };
                        let id = uuid::Uuid::new_v4().to_string();
                        let suggested =
                            crate::ui::utils::suggest_pattern(&req.tool, &req.input);
                        let proxy = Arc::new(PermAsk {
                            id: id.clone(),
                            tool: req.tool.to_string(),
                            input: req.input.clone(),
                            suggested,
                        });
                        pending.insert(id, req.reply);
                        let _ = ask_bc.send(proxy);
                    }
                    dec = dec_rx.recv() => {
                        let Some(dec) = dec else { break };
                        if let Some(reply) = pending.remove(&dec.id) {
                            let _ = reply.send(dec.decision);
                        }
                    }
                }
            }
        });
    }

    let WebArgs {
        cli,
        cfg,
        context,
        client,
        mut session,
        permission,
        ask_tx,
        sandbox,
        provider,
        model,
        ask_rx: _,
    } = args;
    session.refresh_git_branch();

    let defaults = Defaults {
        provider: provider.clone(),
        model: model.clone(),
        context_window: session.context_window,
    };
    let reasoning = cfg.resolve_show_reasoning();
    let initial_prompt = session.prompt.as_ref().map(|p| p.name.to_string());

    let state = Arc::new(AppState {
        cli,
        cfg,
        context,
        runtime: AsyncRwLock::new(RuntimeModel {
            client,
            provider,
            model,
        }),
        ui: AsyncRwLock::new(UiState {
            prompt: initial_prompt,
            reasoning,
            pre_prompt_mode: None,
        }),
        defaults,
        permission,
        ask_tx,
        sandbox,
        seed: AsyncMutex::new(Some(session)),
        ask_bc,
        decisions_tx: dec_tx,
        models_overlay: AsyncRwLock::new(Vec::new()),
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/ws", get(ws_handler))
        .route("/upload", post(upload))
        .layer(DefaultBodyLimit::max(25 * 1024 * 1024))
        .with_state(state);

    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("web ui listening on http://{}", addr);
    println!("zerostack web ui: http://{}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}

// ── static assets ────────────────────────────────────────────────────────

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_js() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        APP_JS,
    )
}

async fn style_css() -> impl IntoResponse {
    ([(axum::http::header::CONTENT_TYPE, "text/css")], STYLE_CSS)
}

/// `POST /upload`: raw body + `x-filename` header. Files land in
/// `<cwd>/.zerostack-uploads/` and come back as a cwd-relative path the agent
/// can read directly.
async fn upload(
    State(_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // HTTP header values are bytes, so non-ASCII names arrive percent-encoded
    // (the browser side sends encodeURIComponent(name)).
    let raw = match headers.get("x-filename") {
        Some(v) => percent_decode(v.as_bytes()),
        None => String::new(),
    };
    let name = sanitize_filename(&raw);
    let base = std::env::current_dir().unwrap_or_default();
    let dir = base.join(".zerostack-uploads");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            json!({"ok": false, "error": e.to_string()}).to_string(),
        );
    }
    let stamp = chrono::Utc::now().format("%H%M%S").to_string();
    let path = dir.join(format!("{stamp}-{name}"));
    match tokio::fs::write(&path, &body).await {
        Ok(()) => {
            let rel = path
                .strip_prefix(&base)
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| name.clone());
            (
                axum::http::StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                json!({"ok": true, "path": rel, "bytes": body.len()}).to_string(),
            )
        }
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            json!({"ok": false, "error": e.to_string()}).to_string(),
        ),
    }
}

/// Minimal percent-decoder (RFC 3986 + `+` → space): filenames can be any
/// UTF-8, header values cannot.
fn percent_decode(bytes: &[u8]) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = |c: u8| -> Option<u8> {
                    match c {
                        b'0'..=b'9' => Some(c - b'0'),
                        b'a'..=b'f' => Some(c - b'a' + 10),
                        b'A'..=b'F' => Some(c - b'A' + 10),
                        _ => None,
                    }
                };
                match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push(h * 16 + l);
                        i += 3;
                    }
                    _ => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

/// Keep the basename and a conservative charset: an uploaded name must never
/// escape the upload directory.
fn sanitize_filename(raw: &str) -> String {
    let base = raw.rsplit(['/', '\\']).next().unwrap_or(raw);
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || matches!(c, '.' | '-' | '_' | ' ') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let cleaned = cleaned.trim_matches('.').to_string();
    if cleaned.is_empty() {
        "upload.bin".to_string()
    } else {
        cleaned
    }
}

// ── websocket ────────────────────────────────────────────────────────────

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| connection(state, socket))
}

/// Per-connection turn bookkeeping. The runner's events are forwarded by a
/// dedicated task into the connection's control channel; `abort` kills the
/// agent stream for real (it is what keeps a superseded run from executing
/// tools invisibly), `fwd` reaps the forwarder.
struct Turn {
    abort: tokio::task::AbortHandle,
    fwd: tokio::task::JoinHandle<()>,
    /// Session length before the optimistic user message, for rollback on
    /// `AgentEvent::Error` (mirrors `Engine::run_agent_text`).
    user_msg_len: usize,
    /// Unanswered permission ask ids; denied at turn end so tools never hang.
    pending_asks: Vec<String>,
}

async fn connection(state: Arc<AppState>, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Value>(512);
    let forwarder = tokio::spawn(async move {
        while let Some(v) = out_rx.recv().await {
            if sink.send(WsMessage::Text(v.to_string().into())).await.is_err() {
                break;
            }
        }
    });

    // Inbound reader: WS frames → control channel (keeps the main loop free
    // of borrow conflicts between the stream and turn receivers).
    let (ctl_tx, mut ctl_rx) = mpsc::channel::<ConnMsg>(256);
    let reader_tx = ctl_tx.clone();
    let reader = tokio::spawn(async move {
        loop {
            match stream.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                    if reader_tx.send(ConnMsg::Inbound(v)).await.is_err() {
                        let _ = reader_tx.send(ConnMsg::Disconnected).await;
                        return;
                    }
                }
                Some(Ok(_)) => {}
                // Stream errored or ended: tell the main loop to exit.
                _ => {
                    let _ = reader_tx.send(ConnMsg::Disconnected).await;
                    return;
                }
            }
        }
    });

    let send = {
        let tx = out_tx.clone();
        move |v: Value| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(v).await;
            }
        }
    };

    // Each connection owns a conversation: the seeded session for the first
    // client, a fresh one afterwards.
    let mut session = {
        let mut seed = state.seed.lock().await;
        match seed.take() {
            Some(s) => s,
            None => Session::new(
                &state.defaults.provider,
                &state.defaults.model,
                state.defaults.context_window,
                "",
            ),
        }
    };
    if let Ok(cwd) = std::env::current_dir() {
        session.working_dir = CompactString::new(cwd.to_string_lossy());
    }
    session.refresh_git_branch();
    let conn = Arc::new(Conn {
        session: AsyncMutex::new(session),
        busy: AtomicBool::new(false),
        loop_run: AsyncMutex::new(None),
        attached: AsyncMutex::new(Vec::new()),
    });

    let mut turn: Option<Turn> = None;
    let mut pending_calls: Vec<(CompactString, u64)> = Vec::new();
    let mut ask_sub = state.ask_bc.subscribe();

    // Initial payload: full session + control surface, so the client can
    // render history, the session list and every dropdown in one shot.
    {
        let session = conn.session.lock().await;
        send(session_state(&session, &state)).await;
        drop(session);
        send(ui_state(&state)).await;
        send(session_list_payload()).await;
        send(attached_payload(&conn).await).await;
    }

    loop {
        tokio::select! {
            ctl = ctl_rx.recv() => {
                let Some(msg) = ctl else { break };
                match msg {
                    ConnMsg::Inbound(v) => {
                        handle_inbound(&state, &conn, v, &mut turn, &send, &ctl_tx, &out_tx).await;
                    }
                    ConnMsg::Agent(event) => {
                        let done = if let Some(t) = turn.as_mut() {
                            handle_event(&state, &conn, t, event, &mut pending_calls, &send).await
                        } else {
                            false
                        };
                        if done {
                            retire_turn(&state, &conn, &mut turn).await;
                            maybe_next_loop(&state, &conn, &mut turn, &send, &ctl_tx).await;
                        }
                    }
                    ConnMsg::AgentClosed => {
                        if let Some(t) = turn.take() {
                            t.abort.abort();
                            t.fwd.abort();
                            deny_pending(&state, &t.pending_asks).await;
                            conn.busy.store(false, Ordering::SeqCst);
                            send(json!({"type": "turn_end", "cancelled": true})).await;
                        }
                    }
                    ConnMsg::Disconnected => break,
                }
            }
            ask = ask_sub.recv() => {
                match ask {
                    Ok(p) => {
                        if let Some(t) = turn.as_mut() {
                            t.pending_asks.push(p.id.clone());
                            send(json!({
                                "type": "permission_ask",
                                "id": p.id,
                                "tool": p.tool,
                                "input": p.input,
                                "suggested_pattern": p.suggested,
                            })).await;
                        }
                        // Asks outside a turn are impossible (tools only run
                        // inside one); drop silently if one slips through.
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }
        }
    }

    if let Some(t) = turn.take() {
        t.abort.abort();
        t.fwd.abort();
        deny_pending(&state, &t.pending_asks).await;
        conn.busy.store(false, Ordering::SeqCst);
    }
    reader.abort();
    forwarder.abort();
}

/// Handle one inbound browser message. Turn completion is signalled by
/// `handle_event` (Done/Error), never here.
async fn handle_inbound<F, Fut>(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    v: Value,
    turn: &mut Option<Turn>,
    send: &F,
    ctl_tx: &mpsc::Sender<ConnMsg>,
    out_tx: &mpsc::Sender<Value>,
) where
    F: Fn(Value) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    match v.get("type").and_then(Value::as_str) {
        Some("prompt") => {
            let Some(text) = v.get("text").and_then(Value::as_str) else { return; };
            if text.trim().is_empty() {
                return;
            }
            begin_turn(state, conn, turn, send, ctl_tx, text.to_string()).await;
        }
        Some("cancel") => {
            if let Some(t) = turn.take() {
                t.abort.abort();
                t.fwd.abort();
                deny_pending(state, &t.pending_asks).await;
                conn.busy.store(false, Ordering::SeqCst);
                send(json!({"type": "turn_end", "cancelled": true})).await;
            }
        }
        Some("permission_reply") => {
            let Some(id) = v.get("id").and_then(Value::as_str) else { return; };
            let Some(d) = v.get("decision").and_then(Value::as_str) else { return; };
            if let Some(t) = turn.as_mut() {
                t.pending_asks.retain(|p| p != id);
            }
            let decision = match d {
                "allow_always" => {
                    let pattern = v
                        .get("pattern")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    UserDecision::AllowAlways(pattern)
                }
                "deny" => UserDecision::Deny,
                _ => UserDecision::AllowOnce,
            };
            let _ = state
                .decisions_tx
                .send(Decision {
                    id: id.to_string(),
                    decision,
                })
                .await;
        }
        Some("new_session") => {
            if turn.is_some() {
                send(json!({"type": "error", "message": "cannot switch sessions during a turn"}))
                    .await;
                return;
            }
            let s = new_session(state, conn).await;
            let payload = session_state(&s, state);
            *conn.session.lock().await = s;
            send(payload).await;
        }
        Some("resume") => {
            let Some(id) = v.get("id").and_then(Value::as_str) else { return; };
            if turn.is_some() {
                send(json!({"type": "error", "message": "cannot switch sessions during a turn"}))
                    .await;
                return;
            }
            match storage::find_sessions_by_prefix(id) {
                Ok(list) if !list.is_empty() => {
                    let s = list.into_iter().next().expect("non-empty");
                    let payload = session_state(&s, state);
                    *conn.session.lock().await = s;
                    send(payload).await;
                }
                Ok(_) => {
                    send(json!({"type": "error", "message": format!("session not found: {id}")}))
                        .await;
                }
                Err(e) => send(json!({"type": "error", "message": e.to_string()})).await,
            }
        }
        Some("delete_session") => {
            let Some(id) = v.get("id").and_then(Value::as_str) else {
                return;
            };
            // An empty id would prefix-match every session (including the live
            // one); refuse rather than risk deleting the wrong conversation.
            if id.is_empty() {
                send(json!({"type": "error", "message": "delete_session requires an id"}))
                    .await;
                return;
            }
            if turn.is_some() {
                send(json!({"type": "error", "message": "cannot delete a session during a turn"}))
                    .await;
                return;
            }
            // The sidebar sends a full id, but resolve through the same prefix
            // matcher `resume` uses. Ambiguity is an error: a partial id must
            // never silently delete a different conversation.
            let active_id = conn.session.lock().await.id.clone();
            let target_id = if active_id.starts_with(id) {
                // A freshly created session has never been written to disk, so
                // lookups below would miss it. Match the live session first.
                Some(active_id.clone())
            } else {
                match storage::find_sessions_by_prefix(id) {
                    Err(e) => {
                        send(json!({"type": "error", "message": e.to_string()})).await;
                        return;
                    }
                    Ok(m) => match m.len() {
                        0 => None,
                        1 => Some(m.into_iter().next().expect("non-empty").id),
                        n => {
                            send(json!({
                                "type": "error",
                                "message": format!("{n} sessions match '{id}', be more specific"),
                            }))
                            .await;
                            return;
                        }
                    },
                }
            };
            let Some(target_id) = target_id else {
                send(json!({"type": "error", "message": format!("session not found: {id}")}))
                    .await;
                return;
            };
            // `new_session` flushes the outgoing session to disk, so it must run
            // before the delete or the file we just removed reappears.
            if target_id == active_id {
                let s = new_session(state, conn).await;
                let payload = session_state(&s, state);
                *conn.session.lock().await = s;
                send(payload).await;
            }
            if let Err(e) = storage::delete_session(&target_id) {
                send(json!({"type": "error", "message": format!("failed to delete: {e}")}))
                    .await;
                return;
            }
            send(json!({"type": "session_deleted", "id": target_id})).await;
            send(session_list_payload()).await;
        }
        Some("session_list") => {
            send(session_list_payload()).await;
        }
        Some("rename_session") => {
            let Some(name) = v.get("name").and_then(Value::as_str) else {
                return;
            };
            let name = name.trim().to_string();
            if turn.is_some() {
                send(json!({"type": "error", "message": "cannot rename during a turn"})).await;
                return;
            }
            {
                let mut session = conn.session.lock().await;
                session.name = CompactString::new(&name);
                save_best_effort(&state.cli, &session);
            }
            let session = conn.session.lock().await;
            send(session_state(&session, state)).await;
            drop(session);
            send(session_list_payload()).await;
        }
        Some("add_file") => {
            let Some(raw) = v.get("path").and_then(Value::as_str) else {
                return;
            };
            let base = std::env::current_dir().unwrap_or_default();
            let p = std::path::PathBuf::from(raw.trim());
            let abs = if p.is_absolute() { p } else { base.join(&p) };
            match std::fs::canonicalize(&abs) {
                Ok(c) if c.is_file() && c.starts_with(&base) => {
                    let rel = c
                        .strip_prefix(&base)
                        .map(|x| x.to_string_lossy().to_string())
                        .unwrap_or_else(|_| raw.to_string());
                    let mut list = conn.attached.lock().await;
                    if !list.contains(&rel) {
                        list.push(rel.clone());
                    }
                    drop(list);
                    send(attached_payload(conn).await).await;
                }
                Ok(_) => {
                    send(json!({"type": "error", "message": format!("不在工作目录内或不是文件: {raw}")}))
                        .await
                }
                Err(e) => {
                    send(json!({"type": "error", "message": format!("{raw}: {e}")})).await
                }
            }
        }
        Some("remove_file") => {
            let Some(raw) = v.get("path").and_then(Value::as_str) else {
                return;
            };
            conn.attached.lock().await.retain(|p| p != raw);
            send(attached_payload(conn).await).await;
        }
        Some("list_attached") => {
            send(attached_payload(conn).await).await;
        }
        Some("export_session") => {
            let session = conn.session.lock().await;
            let body = export_markdown(&session);
            let filename = format!(
                "zerostack-{}.md",
                session.id.chars().take(8).collect::<String>()
            );
            drop(session);
            send(json!({
                "type": "export",
                "filename": filename,
                "content": body,
            }))
            .await;
        }
        Some("set_prompt") => {
            let name = v
                .get("name")
                .and_then(Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty() && s != "default");
            if let Some(n) = &name
                && !state.context.prompts.contains_key(n)
            {
                send(json!({"type": "error", "message": format!("unknown prompt: {n}")}))
                    .await;
                return;
            }
            let prev_mode = state.permission.as_ref().map(|p| {
                p.lock().unwrap_or_else(|e| e.into_inner()).mode()
            });
            {
                let mut session = conn.session.lock().await;
                match &name {
                    Some(n) => {
                        // Shared with the TUI/Engine: sets session.prompt to the
                        // right PromptRef and applies any `%%mode=` directive.
                        let mut ctx = state.context.clone();
                        crate::ui::apply_prompt_mode(n, &mut ctx, &mut session, &state.permission);
                        state.ui.write().await.pre_prompt_mode = prev_mode;
                    }
                    None => {
                        session.prompt = None;
                        let restore = state.ui.write().await.pre_prompt_mode.take();
                        if let (Some(m), Some(p)) = (restore, &state.permission) {
                            p.lock().unwrap_or_else(|e| e.into_inner()).set_mode(m);
                        }
                    }
                }
                save_best_effort(&state.cli, &session);
            }
            state.ui.write().await.prompt = name;
            send(ui_state(state)).await;
        }
        Some("set_mode") => {
            let Some(m) = v.get("mode").and_then(Value::as_str) else {
                return;
            };
            let Some(mode) = SecurityMode::from_str(m) else {
                send(json!({"type": "error", "message": format!("unknown mode: {m}")})).await;
                return;
            };
            match &state.permission {
                Some(p) => {
                    p.lock().unwrap_or_else(|e| e.into_inner()).set_mode(mode);
                    send(ui_state(state)).await;
                }
                None => {
                    send(json!({"type": "error", "message": "permission system disabled"})).await
                }
            }
        }
        Some("set_cwd") => {
            let Some(raw) = v.get("path").and_then(Value::as_str) else {
                return;
            };
            if turn.is_some() {
                send(json!({"type": "error", "message": "cannot change cwd during a turn"}))
                    .await;
                return;
            }
            let expanded = expand_path(raw.trim());
            let path = std::path::PathBuf::from(&expanded);
            let abs = match std::fs::canonicalize(&path) {
                Ok(p) if p.is_dir() => p,
                Ok(_) => {
                    send(json!({"type": "error", "message": format!("not a directory: {expanded}")}))
                        .await;
                    return;
                }
                Err(e) => {
                    send(json!({"type": "error", "message": format!("{expanded}: {e}")})).await;
                    return;
                }
            };
            if let Err(e) = std::env::set_current_dir(&abs) {
                send(json!({"type": "error", "message": format!("chdir failed: {e}")}))
                    .await;
                return;
            }
            {
                let mut session = conn.session.lock().await;
                session.working_dir = CompactString::new(abs.to_string_lossy());
                session.refresh_git_branch();
                save_best_effort(&state.cli, &session);
                let payload = session_state(&session, state);
                drop(session);
                send(payload).await;
            }
            send(ui_state(state)).await;
        }
        Some("set_reasoning") => {
            let enabled = v.get("enabled").and_then(Value::as_bool).unwrap_or(false);
            state.ui.write().await.reasoning = enabled;
            send(ui_state(state)).await;
        }
        Some("switch_model") => {
            let Some(name) = v.get("name").and_then(Value::as_str) else {
                return;
            };
            // UI-added models live in the overlay and win over config entries.
            let resolved = {
                let ov = state.models_overlay.read().await;
                ov.iter()
                    .find(|(n, _, _)| n == name)
                    .map(|(_, p, m)| (p.clone(), m.clone()))
            };
            let resolved = match resolved {
                Some(x) => Some(x),
                None => {
                    let qm = crate::config::quick_models_map_ref(&state.cfg);
                    qm.get(name).map(|q| (q.provider.to_string(), q.model.to_string()))
                }
            };
            let Some((prov, mdl)) = resolved else {
                send(json!({"type": "error", "message": format!("unknown model: {name}")})).await;
                return;
            };
            // Rebuild the client for the target provider, then hot-swap the
            // runtime triple. A turn already in flight keeps its own model;
            // the next prompt picks this one up.
            match crate::provider::create_client(
                prov.as_str(),
                state.cli.api_key.as_deref(),
                &state.cfg.custom_providers_map(),
                state.cfg.api_keys.as_ref(),
            ) {
                Ok(client) => {
                    {
                        let mut rt = state.runtime.write().await;
                        rt.client = client;
                        rt.provider = prov.clone();
                        rt.model = mdl.clone();
                    }
                    {
                        let mut session = conn.session.lock().await;
                        session.provider = CompactString::new(&prov);
                        session.model = CompactString::new(&mdl);
                    }
                    let session = conn.session.lock().await;
                    send(session_state(&session, state)).await;
                }
                Err(e) => send(json!({"type": "error", "message": e.to_string()})).await,
            }
        }

        Some("list_dir") => {
            let rel = v.get("path").and_then(Value::as_str).unwrap_or(".").to_string();
            let base = std::env::current_dir().unwrap_or_default();
            let target = if rel == "." || rel.is_empty() {
                base.clone()
            } else {
                base.join(rel.trim_start_matches('/'))
            };
            let dir = match std::fs::canonicalize(&target) {
                Ok(d) if d.is_dir() && d.starts_with(&base) => d,
                _ => {
                    send(json!({"type": "error", "message": format!("无法读取目录: {rel}")}))
                        .await;
                    return;
                }
            };
            let mut entries: Vec<Value> = Vec::new();
            if let Ok(rd) = std::fs::read_dir(&dir) {
                for e in rd.flatten() {
                    let name = e.file_name().to_string_lossy().to_string();
                    if matches!(name.as_str(), ".git" | "target" | "node_modules" | ".venv") {
                        continue;
                    }
                    let p = e.path();
                    let relp = p
                        .strip_prefix(&base)
                        .map(|x| x.to_string_lossy().to_string())
                        .unwrap_or_else(|_| name.clone());
                    entries.push(json!({
                        "name": name,
                        "path": relp,
                        "dir": p.is_dir(),
                    }));
                }
            }
            entries.sort_by(|a, b| {
                let ad = a["dir"].as_bool().unwrap_or(false);
                let bd = b["dir"].as_bool().unwrap_or(false);
                bd.cmp(&ad).then_with(|| {
                    a["name"]
                        .as_str()
                        .unwrap_or("")
                        .to_lowercase()
                        .cmp(&b["name"].as_str().unwrap_or("").to_lowercase())
                })
            });
            send(json!({"type": "dir_list", "path": rel, "entries": entries})).await;
        }
        Some("complete_path") => {
            let prefix = v
                .get("prefix")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim_start_matches('@')
                .to_lowercase();
            let base = std::env::current_dir().unwrap_or_default();
            let mut out: Vec<String> = Vec::new();
            for entry in ignore::WalkBuilder::new(&base)
                .max_depth(Some(6))
                .git_ignore(true)
                .hidden(false)
                .build()
                .flatten()
            {
                if out.len() >= 40 {
                    break;
                }
                let p = entry.path();
                if !p.is_file() {
                    continue;
                }
                let rel = p
                    .strip_prefix(&base)
                    .map(|x| x.to_string_lossy().to_string())
                    .unwrap_or_default();
                if rel.is_empty() || rel.starts_with("target/") {
                    continue;
                }
                if prefix.is_empty() || rel.to_lowercase().contains(&prefix) {
                    out.push(rel);
                }
            }
            out.sort();
            send(json!({"type": "path_matches", "items": out})).await;
        }
        Some("undo_turn") => {
            if turn.is_some() {
                send(json!({"type": "error", "message": "正在运行,稍后再撤销"})).await;
                return;
            }
            let session = conn.session.lock().await;
            let payload = {
                let mut s = session;
                match s.messages.iter().rposition(|m| m.role == MessageRole::User) {
                    Some(idx) => {
                        s.truncate_to(idx);
                        save_best_effort(&state.cli, &s);
                        session_state(&s, state)
                    }
                    None => json!({"type": "error", "message": "没有可撤销的轮次"}),
                }
            };
            send(payload).await;
        }
        Some("retry_turn") => {
            if turn.is_some() {
                send(json!({"type": "error", "message": "正在运行,稍后再重试"})).await;
                return;
            }
            let text = {
                let session = conn.session.lock().await;
                session
                    .messages
                    .iter()
                    .rposition(|m| m.role == MessageRole::User)
                    .map(|idx| (idx, session.messages[idx].content.to_string()))
            };
            let Some((idx, text)) = text else {
                send(json!({"type": "error", "message": "没有可重试的消息"})).await;
                return;
            };
            {
                let mut session = conn.session.lock().await;
                session.truncate_to(idx);
                save_best_effort(&state.cli, &session);
            }
            begin_turn(state, conn, turn, send, ctl_tx, text).await;
        }
        Some("compact") => {
            if turn.is_some() {
                send(json!({"type": "error", "message": "正在运行,稍后再压缩"})).await;
                return;
            }
            // Mechanical compaction: keep the recent tail verbatim and replace
            // the head with a deterministic digest of the dropped turns.
            let payload = {
                let mut session = conn.session.lock().await;
                let total = session.messages.len();
                // Keep the tail verbatim, but never refuse just because the
                // conversation is short: compress whenever there are at least
                // three turns worth of messages.
                let keep = if total > 12 { 8 } else { total / 2 };
                let drop_n = total.saturating_sub(keep);
                if drop_n == 0 {
                    None
                } else {
                    let mut digest = String::from("[已压缩的历史对话摘要]\n");
                    for m in session.messages.iter().take(drop_n) {
                        let tag = match m.role {
                            MessageRole::User => "用户",
                            MessageRole::Assistant => "助手",
                            MessageRole::System => "系统",
                            MessageRole::ToolCall => "工具调用",
                            MessageRole::ToolResult => "工具结果",
                            MessageRole::SubagentToolCall => "子代理",
                        };
                        let line: String = m.content.chars().take(200).collect();
                        digest.push_str(&format!("- {tag}: {line}\n"));
                    }
                    let before = session.total_estimated_tokens;
                    session.compress(digest, drop_n, 0);
                    save_best_effort(&state.cli, &session);
                    let payload = session_state(&session, state);
                    tracing::info!(
                        "web compact: dropped {drop_n} messages, est tokens {before} -> {}",
                        session.total_estimated_tokens
                    );
                    Some(payload)
                }
            };
            match payload {
                Some(p) => send(p).await,
                None => {
                    send(json!({"type": "error", "message": "消息太少,无需压缩"})).await
                }
            }
        }
        Some("config_get") => {
            send(config_payload(state).await).await;
        }
        Some("add_model") => {
            let name = v.get("name").and_then(Value::as_str).unwrap_or("").trim().to_string();
            let provider = v
                .get("provider")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let model = v.get("model").and_then(Value::as_str).unwrap_or("").trim().to_string();
            if name.is_empty() || provider.is_empty() || model.is_empty() {
                send(json!({"type": "error", "message": "name/provider/model 都要填"})).await;
                return;
            }
            {
                let mut ov = state.models_overlay.write().await;
                ov.retain(|(n, _, _)| n != &name);
                ov.push((name.clone(), provider.clone(), model.clone()));
            }
            // Persist so the entry survives a restart. Never append a section
            // that already exists: duplicate tables make the config unparseable.
            let path = crate::config::config_file_path();
            let existing = std::fs::read_to_string(&path).unwrap_or_default();
            if !existing.contains(&format!("[quick_models.{name}]")) {
                let block = format!(
                    "\n[quick_models.{name}]\nprovider = \"{provider}\"\nmodel = \"{model}\"\n"
                );
                if let Ok(mut f) =
                    std::fs::OpenOptions::new().append(true).create(true).open(&path)
                {
                    use std::io::Write;
                    let _ = f.write_all(block.as_bytes());
                }
            }
            send(config_payload(state).await).await;
        }
        Some("remove_model") => {
            let Some(name) = v.get("name").and_then(Value::as_str) else { return; };
            state.models_overlay.write().await.retain(|(n, _, _)| n != name);
            send(config_payload(state).await).await;
        }
        Some("btw") => {
            let Some(text) = v.get("text").and_then(Value::as_str) else { return; };
            if text.trim().is_empty() {
                return;
            }
            // Read-only side question: own agent, own permission checker, own
            // history snapshot; nothing is written back to the session.
            let (model_str, model) = {
                let rt = state.runtime.read().await;
                (rt.model.clone(), rt.client.completion_model(rt.model.clone()))
            };
            let temperature = crate::config::resolve_temperature(&state.cli, &state.cfg, &model_str);
            let extra_body = crate::config::resolve_extra_body(&state.cfg, &model_str);
            let perm_config = state.cfg.build_permission_config();
            let checker = PermissionChecker::new(
                &perm_config,
                SecurityMode::ReadOnly,
                std::env::current_dir().ok(),
                state.cfg.permission_modes.clone(),
            );
            let perm: PermCheck = Arc::new(std::sync::Mutex::new(checker));
            let (ctx, reasoning) = {
                let ui = state.ui.read().await;
                let mut c = state.context.clone();
                if let Some(name) = &ui.prompt
                    && let Some(body) = state.context.prompts.get(name)
                {
                    let (_, clean) = crate::permission::parse_prompt_mode(body);
                    c.current_prompt = Some(clean.to_string());
                    c.current_prompt_name = Some(name.clone());
                }
                (c, ui.reasoning)
            };
            let history = {
                let session = conn.session.lock().await;
                runner::convert_history(&session)
            };
            let agent = build_agent(
                model,
                AgentBuild {
                    cli: &state.cli,
                    cfg: &state.cfg,
                    context: &ctx,
                    permission: Some(perm),
                    ask_tx: None,
                    sandbox: state.sandbox.clone(),
                    reasoning_enabled: reasoning,
                    temperature,
                    extra_body,
                    #[cfg(feature = "mcp")]
                    mcp_manager: None,
                },
            )
            .await;
            let AgentRunner { mut event_rx, abort_handle } = agent
                .spawn_runner(
                    text.to_string(),
                    history,
                    state.cfg.retry.clone(),
                    #[cfg(feature = "hooks")]
                    None,
                )
                .await;
            let tx = out_tx.clone();
            tokio::spawn(async move {
                while let Some(ev) = event_rx.recv().await {
                    let payload = match ev {
                        AgentEvent::Token(t) => Some(json!({"type": "btw_token", "text": t})),
                        AgentEvent::ToolCall { name, .. } => {
                            Some(json!({"type": "btw_tool", "name": name}))
                        }
                        AgentEvent::Done { response, .. } => {
                            Some(json!({"type": "btw_done", "text": response}))
                        }
                        AgentEvent::Error(e) => Some(json!({"type": "btw_done", "error": e})),
                        _ => None,
                    };
                    if let Some(p) = payload
                        && tx.send(p).await.is_err()
                    {
                        break;
                    }
                }
                let _ = tx.send(json!({"type": "btw_end"})).await;
                abort_handle.abort();
            });
        }
        Some("loop_start") => {
            if turn.is_some() {
                send(json!({"type": "error", "message": "正在运行,先停止当前轮次"})).await;
                return;
            }
            let Some(prompt) = v.get("prompt").and_then(Value::as_str) else { return; };
            let prompt = prompt.trim().to_string();
            if prompt.is_empty() {
                send(json!({"type": "error", "message": "用法: /loop <任务>"})).await;
                return;
            }
            let max = v.get("max").and_then(Value::as_u64).unwrap_or(10).max(1) as u32;
            {
                let mut g = conn.loop_run.lock().await;
                *g = Some(LoopRun {
                    prompt: prompt.clone(),
                    iteration: 0,
                    max,
                });
            }
            send(loop_status(conn).await).await;
            let text = loop_iteration_prompt(&prompt, 1, max);
            {
                let mut g = conn.loop_run.lock().await;
                if let Some(l) = g.as_mut() {
                    l.iteration = 1;
                }
            }
            begin_turn(state, conn, turn, send, ctl_tx, text).await;
        }
        Some("loop_stop") => {
            *conn.loop_run.lock().await = None;
            send(loop_status(conn).await).await;
        }
        #[cfg(feature = "git-worktree")]
        Some("worktree_new") => {
            let Some(name) = v.get("name").and_then(Value::as_str) else { return; };
            match crate::extras::git_worktree::create(name.trim(), None) {
                Ok((path, _info)) => {
                    if std::env::set_current_dir(&path).is_ok() {
                        let mut session = conn.session.lock().await;
                        session.working_dir = CompactString::new(path.to_string_lossy());
                        session.refresh_git_branch();
                        save_best_effort(&state.cli, &session);
                        let payload = session_state(&session, state);
                        drop(session);
                        send(payload).await;
                    }
                    send(json!({
                        "type": "worktree",
                        "ok": true,
                        "message": format!("已切到 worktree: {}", path.display()),
                    }))
                    .await;
                    send(ui_state(state)).await;
                }
                Err(e) => {
                    send(json!({"type": "worktree", "ok": false, "message": e})).await;
                }
            }
        }
        #[cfg(feature = "git-worktree")]
        Some("worktree_merge") => {
            let target = v
                .get("target")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
            match crate::extras::git_worktree::detect() {
                Some(info) => {
                    let target = target.unwrap_or_else(|| {
                        crate::extras::git_worktree::default_branch(&info.main_repo_path)
                            .unwrap_or_else(|| "main".to_string())
                    });
                    let (_st, outcome) = crate::extras::git_worktree::try_merge(&info, &target);
                    match outcome {
                        crate::extras::git_worktree::MergeOutcome::Success => {
                            let _ = std::env::set_current_dir(&info.main_repo_path);
                            crate::extras::git_worktree::cleanup_worktree(
                                &info.worktree_path.to_string_lossy(),
                                &info.branch,
                                &info.main_repo_path.to_string_lossy(),
                                false,
                            );
                            {
                                let mut session = conn.session.lock().await;
                                session.working_dir =
                                    CompactString::new(info.main_repo_path.to_string_lossy());
                                session.refresh_git_branch();
                                save_best_effort(&state.cli, &session);
                                let payload = session_state(&session, state);
                                drop(session);
                                send(payload).await;
                            }
                            send(json!({
                                "type": "worktree",
                                "ok": true,
                                "message": format!("已合并 {} 到 {} 并清理 worktree", info.branch, target),
                            }))
                            .await;
                            send(ui_state(state)).await;
                        }
                        crate::extras::git_worktree::MergeOutcome::Conflicts(files) => {
                            send(json!({
                                "type": "worktree",
                                "ok": false,
                                "message": format!("合并冲突({} 个文件): {}", files.len(), files.join(", ")),
                            }))
                            .await;
                        }
                        crate::extras::git_worktree::MergeOutcome::Error(e) => {
                            send(json!({"type": "worktree", "ok": false, "message": e})).await;
                        }
                    }
                }
                None => {
                    send(json!({
                        "type": "worktree",
                        "ok": false,
                        "message": "当前目录不是 worktree",
                    }))
                    .await;
                }
            }
        }
        #[cfg(feature = "git-worktree")]
        Some("worktree_exit") => match crate::extras::git_worktree::detect() {
            Some(info) => {
                let _ = std::env::set_current_dir(&info.main_repo_path);
                {
                    let mut session = conn.session.lock().await;
                    session.working_dir =
                        CompactString::new(info.main_repo_path.to_string_lossy());
                    session.refresh_git_branch();
                    save_best_effort(&state.cli, &session);
                    let payload = session_state(&session, state);
                    drop(session);
                    send(payload).await;
                }
                send(json!({
                    "type": "worktree",
                    "ok": true,
                    "message": format!("已回到主仓库(分支 {} 保留)", info.branch),
                }))
                .await;
                send(ui_state(state)).await;
            }
            None => {
                send(json!({"type": "worktree", "ok": false, "message": "当前目录不是 worktree"}))
                    .await;
            }
        },
        _ => {}
    }
}

/// Forward runner events to the connection's control channel until the
/// channel closes. The `Done`/`Error` events themselves travel through here;
/// the main loop detects turn completion by their return value.
async fn forward_runner(mut rx: mpsc::Receiver<AgentEvent>, ctl: mpsc::Sender<ConnMsg>) {
    while let Some(event) = rx.recv().await {
        if ctl.send(ConnMsg::Agent(event)).await.is_err() {
            return;
        }
    }
    let _ = ctl.send(ConnMsg::AgentClosed).await;
}

/// Retire a finished (Done/Error) turn: reap the forwarder, deny any
/// still-unanswered permission asks, release the busy flag.
async fn retire_turn(state: &Arc<AppState>, conn: &Arc<Conn>, turn: &mut Option<Turn>) {
    if let Some(t) = turn.take() {
        t.fwd.abort();
        deny_pending(state, &t.pending_asks).await;
        conn.busy.store(false, Ordering::SeqCst);
    }
}

/// Returns `true` when the turn is over (Done or Error).
async fn handle_event<F, Fut>(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    turn: &mut Turn,
    event: AgentEvent,
    pending_calls: &mut Vec<(CompactString, u64)>,
    send: &F,
) -> bool
where
    F: Fn(Value) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    match event {
        AgentEvent::Token(text) => {
            let safe = crate::ui::events::sanitize_output(&text);
            send(json!({"type": "token", "text": safe})).await;
            false
        }
        AgentEvent::Reasoning(text) => {
            if state.ui.read().await.reasoning {
                send(json!({"type": "reasoning", "text": text})).await;
            }
            false
        }
        AgentEvent::ToolCall {
            call_id: event_id,
            name,
            args,
        } => {
            let summary = crate::ui::utils::format_tool_call_summary(&name, &args);
            let mut session = conn.session.lock().await;
            let call_id = session.add_tool_call(&name, &args);
            pending_calls.push((event_id, call_id));
            save_best_effort(&state.cli, &session);
            drop(session);
            send(json!({
                "type": "tool_call",
                "call_id": call_id,
                "name": name,
                "summary": summary,
                "args": args,
            }))
            .await;
            false
        }
        #[cfg(any(feature = "subagents", feature = "acp"))]
        AgentEvent::SubagentToolCall { name, args } => {
            let summary = crate::ui::utils::format_tool_call_summary(&name, &args);
            let parent = pending_calls.last().map(|&(_, id)| id);
            let mut session = conn.session.lock().await;
            session.add_subagent_tool_call(parent, &name, &args);
            save_best_effort(&state.cli, &session);
            drop(session);
            send(json!({"type": "subagent", "name": name, "summary": summary})).await;
            false
        }
        AgentEvent::ToolResult {
            call_id: event_id,
            name,
            output,
        } => {
            let pos = pending_calls.iter().position(|(e, _)| *e == event_id);
            let call_id = match pos {
                Some(i) => Some(pending_calls.remove(i).1),
                None => pending_calls.last().map(|&(_, id)| id),
            };
            let Some(call_id) = call_id else {
                tracing::warn!("tool result without announced call (id={}); skipping", event_id);
                return false;
            };
            let mut session = conn.session.lock().await;
            let display = session.add_tool_result(call_id, &name, &output);
            save_best_effort(&state.cli, &session);
            drop(session);
            send(json!({
                "type": "tool_result",
                "call_id": call_id,
                "name": name,
                "output": output,
                "display": display,
            }))
            .await;
            false
        }
        AgentEvent::Retrying { attempt, max } => {
            send(json!({"type": "retrying", "attempt": attempt, "max": max})).await;
            false
        }
        AgentEvent::CompletionCall {
            input_tokens,
            output_tokens,
            cached_input_tokens,
            cache_creation_input_tokens,
        } => {
            let mut session = conn.session.lock().await;
            apply_completion_usage(
                &state.cfg,
                &mut session,
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cache_creation_input_tokens,
            );
            let payload = json!({
                "type": "usage",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "totals": session_totals(&session),
                "estimated_context_tokens": session.total_estimated_tokens,
            });
            drop(session);
            send(payload).await;
            false
        }
        AgentEvent::Done {
            response,
            input_tokens,
            output_tokens,
            cached_input_tokens,
            cache_creation_input_tokens,
        } => {
            let usage = TurnUsage {
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cache_creation_input_tokens,
            };
            let mut session = conn.session.lock().await;
            finish_turn(&state.cfg, &mut session, &response, usage);
            let payload = json!({
                "type": "done",
                "response": response,
                "usage": usage_json(usage),
                "totals": session_totals(&session),
                "cost": session.total_cost,
                "estimated_context_tokens": session.total_estimated_tokens,
            });
            save_best_effort(&state.cli, &session);
            drop(session);
            send(payload).await;
            send(json!({"type": "turn_end", "cancelled": false})).await;
            true
        }
        AgentEvent::Error(e) => {
            let mut session = conn.session.lock().await;
            // Roll back the optimistic user message like the TUI does.
            let len = session.messages.len();
            if len > 0 && session.messages[len - 1].role == MessageRole::User {
                session.truncate_to(turn.user_msg_len.min(len));
            }
            save_best_effort(&state.cli, &session);
            let payload = json!({"type": "error", "message": e});
            drop(session);
            send(payload).await;
            send(json!({"type": "turn_end", "cancelled": false})).await;
            true
        }
    }
}

// ── turn setup ───────────────────────────────────────────────────────────

/// Build the agent and spawn one turn. Agent construction mirrors
/// `extras/acp::run_prompt`; session/history handling mirrors
/// `Engine::start_agent_run`.
async fn start_turn(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    prompt: String,
) -> anyhow::Result<(mpsc::Receiver<AgentEvent>, tokio::task::AbortHandle, usize)> {
    let (model_str, model) = {
        let rt = state.runtime.read().await;
        let model_str = rt.model.clone();
        let model = rt.client.completion_model(model_str.clone());
        (model_str, model)
    };

    let temperature =
        crate::config::resolve_temperature(&state.cli, &state.cfg, &model_str);
    let extra_body = crate::config::resolve_extra_body(&state.cfg, &model_str);

    // Per-turn context: the base context plus the prompt mode picked in the UI
    // (`/prompt` equivalent). Cloning keeps the shared registry untouched.
    let (context, reasoning) = {
        let ui = state.ui.read().await;
        let mut ctx = state.context.clone();
        if let Some(name) = &ui.prompt
            && let Some(body) = state.context.prompts.get(name)
        {
            // The `%%mode=` directive was already applied to the permission
            // checker by `set_prompt`; keep it out of the system prompt.
            let (_, clean) = crate::permission::parse_prompt_mode(body);
            ctx.current_prompt = Some(clean.to_string());
            ctx.current_prompt_name = Some(name.clone());
        }
        (ctx, ui.reasoning)
    };
    // Attached files ride the builder's own `extra_files` channel: their
    // contents get inlined into the prompt, exactly like `/add` in the TUI.
    let mut context = context;
    {
        let base = std::env::current_dir().unwrap_or_default();
        for rel in conn.attached.lock().await.iter() {
            let abs = base.join(rel);
            if abs.is_file() && !context.extra_files.contains(&abs) {
                context.extra_files.push(abs);
            }
        }
    }

    let agent = build_agent(
        model,
        AgentBuild {
            cli: &state.cli,
            cfg: &state.cfg,
            context: &context,
            permission: state.permission.clone(),
            ask_tx: state.ask_tx.clone(),
            sandbox: state.sandbox.clone(),
            reasoning_enabled: reasoning,
            temperature,
            extra_body,
            #[cfg(feature = "mcp")]
            mcp_manager: None,
        },
    )
    .await;

    let user_msg_len;
    let history;
    let timestamp;
    {
        let mut session = conn.session.lock().await;
        user_msg_len = session.messages.len();
        session.add_message(MessageRole::User, &prompt);
        timestamp = session.updated_at.clone();
        history = runner::convert_history(&session);
        save_best_effort(&state.cli, &session);
    }
    if !state.cli.no_session
        && let Err(e) = crate::session::chat_history::append_entry(
            &crate::session::chat_history::ChatHistoryEntry {
                content: prompt.clone(),
                timestamp,
            },
        )
    {
        tracing::warn!("failed to append chat history entry: {e}");
    }

    let AgentRunner {
        event_rx,
        abort_handle,
    } = agent
        .spawn_runner(
            prompt,
            history,
            state.cfg.retry.clone(),
            #[cfg(feature = "hooks")]
            None,
        )
        .await;
    Ok((event_rx, abort_handle, user_msg_len))
}

// ── session plumbing ─────────────────────────────────────────────────────

async fn new_session(state: &Arc<AppState>, conn: &Arc<Conn>) -> Session {
    let old = conn.session.lock().await;
    save_best_effort(&state.cli, &old);
    let mut s = Session::new(&old.provider, &old.model, old.context_window, "");
    s.working_dir = old.working_dir.clone();
    s.refresh_git_branch();
    s
}

fn session_summary(s: &Session) -> Value {
    let last = s
        .messages
        .last()
        .map(|m| m.content.chars().take(60).collect::<String>())
        .unwrap_or_default();
    json!({
        "id": s.id,
        "name": s.name,
        "model": s.model,
        "provider": s.provider,
        "updated_at": s.updated_at,
        "messages": s.messages.len(),
        "last": last,
    })
}

/// Quick models (from config) offered to the frontend's model switcher.
fn models_payload(s: &Session, state: &AppState) -> Vec<Value> {
    let qm = crate::config::quick_models_map_ref(&state.cfg);
    let mut names: Vec<&String> = qm.keys().collect();
    names.sort();
    let mut out: Vec<Value> = names
        .iter()
        .map(|n| {
            let q = &qm[*n];
            json!({ "name": n, "provider": q.provider, "model": q.model })
        })
        .collect();
    if out.is_empty() {
        out.push(json!({ "name": s.model, "provider": s.provider, "model": s.model }));
    }
    out
}

fn session_state(s: &Session, state: &AppState) -> Value {
    let messages: Vec<Value> = s
        .messages
        .iter()
        .map(|m| {
            json!({
                "role": m.role,
                "content": m.content,
                "tool": m.tool.as_ref().map(serde_json::to_value).transpose().ok().flatten(),
            })
        })
        .collect();
    json!({
        "type": "session_state",
        "session": {
            "id": s.id,
            "name": s.name,
            "model": s.model,
            "provider": s.provider,
            "working_dir": s.working_dir,
            "context_window": s.context_window,
            "totals": session_totals(s),
            "cost": s.total_cost,
            "estimated_context_tokens": s.total_estimated_tokens,
        },
        "messages": messages,
        "models": models_payload(s, state),
    })
}

fn session_totals(s: &Session) -> Value {
    json!({
        "input_tokens": s.total_input_tokens,
        "output_tokens": s.total_output_tokens,
        "cached_input_tokens": s.total_cached_input_tokens,
        "cache_creation_input_tokens": s.total_cache_creation_input_tokens,
    })
}

fn usage_json(u: TurnUsage) -> Value {
    json!({
        "input_tokens": u.input_tokens,
        "output_tokens": u.output_tokens,
        "cached_input_tokens": u.cached_input_tokens,
        "cache_creation_input_tokens": u.cache_creation_input_tokens,
    })
}

fn save_best_effort(cli: &Cli, session: &Session) {
    if cli.no_session {
        return;
    }
    if let Err(e) = storage::save_session(session) {
        tracing::warn!("failed to save session: {e}");
    }
}

/// Unanswered asks at turn end are denied so tools never hang on a dead
/// dialog.
async fn deny_pending(state: &Arc<AppState>, pending: &[String]) {
    for id in pending {
        let _ = state
            .decisions_tx
            .send(Decision {
                id: id.clone(),
                decision: UserDecision::Deny,
            })
            .await;
    }
}

// ── usage accounting (mirrors `Engine`) ──────────────────────────────────

/// Intermediate-call accounting: cost for every tool round trip (the `Done`
/// event only carries the final call's usage). Mirrors
/// `Engine::apply_completion_usage`.
fn apply_completion_usage(
    cfg: &Config,
    session: &mut Session,
    input_tokens: u64,
    output_tokens: u64,
    cached_input_tokens: u64,
    cache_creation_input_tokens: u64,
) {
    let real = Session::real_input_tokens(
        cfg.is_anthropic_native(&session.provider, &session.model),
        input_tokens,
        cached_input_tokens,
        cache_creation_input_tokens,
    )
    .saturating_add(output_tokens);
    if real > session.total_estimated_tokens {
        session.total_estimated_tokens = real;
    }
    session.total_input_tokens = session.total_input_tokens.saturating_add(input_tokens);
    session.total_output_tokens = session.total_output_tokens.saturating_add(output_tokens);
    session.total_cost += crate::pricing::estimate_cost(
        crate::pricing::billable_input_tokens(
            cfg.is_anthropic_native(&session.provider, &session.model),
            input_tokens,
            cached_input_tokens,
            cache_creation_input_tokens,
        ),
        output_tokens,
        session.input_token_cost,
        session.output_token_cost,
    );
}

/// Fold one finished turn into the session. Mirrors `Engine::finish_turn`
/// minus auto-compaction (headless compaction needs an agent turn of its
/// own; pending a future slash passthrough the web client simply keeps the
/// session under the same limits the TUI enforces interactively).
fn finish_turn(cfg: &Config, session: &mut Session, response: &str, usage: TurnUsage) {
    session.add_message(MessageRole::Assistant, response);
    session.total_input_tokens = session.total_input_tokens.saturating_add(usage.input_tokens);
    session.total_output_tokens = session
        .total_output_tokens
        .saturating_add(usage.output_tokens);
    session.total_cached_input_tokens = session
        .total_cached_input_tokens
        .saturating_add(usage.cached_input_tokens);
    session.total_cache_creation_input_tokens = session
        .total_cache_creation_input_tokens
        .saturating_add(usage.cache_creation_input_tokens);
    session.total_cost += crate::pricing::estimate_cost(
        crate::pricing::billable_input_tokens(
            cfg.is_anthropic_native(&session.provider, &session.model),
            usage.input_tokens,
            usage.cached_input_tokens,
            usage.cache_creation_input_tokens,
        ),
        usage.output_tokens,
        session.input_token_cost,
        session.output_token_cost,
    );
    let context_input_tokens = Session::real_input_tokens(
        cfg.is_anthropic_native(&session.provider, &session.model),
        usage.input_tokens,
        usage.cached_input_tokens,
        usage.cache_creation_input_tokens,
    );
    session.set_calibration(context_input_tokens, usage.output_tokens);
}

// ── control-surface helpers ──────────────────────────────────────────────

/// `~` / `$HOME` expansion for `set_cwd` (the browser has no shell).
fn expand_path(raw: &str) -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    if raw == "~" {
        home
    } else if let Some(rest) = raw.strip_prefix("~/") {
        format!("{home}/{rest}")
    } else if let Some(rest) = raw.strip_prefix("$HOME/") {
        format!("{home}/{rest}")
    } else {
        raw.to_string()
    }
}

/// Markdown transcript for `export_session`.
fn export_markdown(s: &Session) -> String {
    let mut out = String::new();
    out.push_str(&format!("# zerostack session {}\n\n", s.id));
    out.push_str(&format!("- model: `{}` / `{}`\n", s.provider, s.model));
    out.push_str(&format!("- working_dir: `{}`\n", s.working_dir));
    out.push_str(&format!(
        "- created: {}  updated: {}\n",
        s.created_at, s.updated_at
    ));
    out.push_str(&format!(
        "- tokens: in {} / out {} / cost {:.4}\n\n---\n\n",
        s.total_input_tokens, s.total_output_tokens, s.total_cost
    ));
    for m in &s.messages {
        let role = match m.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => "system",
            MessageRole::ToolCall => "tool_call",
            MessageRole::ToolResult => "tool_result",
            MessageRole::SubagentToolCall => "subagent_tool_call",
        };
        out.push_str(&format!("### {role}\n\n{}\n\n", m.content));
    }
    out
}

fn session_list_payload() -> Value {
    let sessions = storage::find_recent_sessions(40)
        .map(|list| list.iter().map(session_summary).collect::<Vec<_>>())
        .unwrap_or_default();
    json!({"type": "session_list", "sessions": sessions})
}

/// Control-surface state: everything the top bar and composer need to render
/// the current prompt mode, permission mode, cwd and reasoning toggle.
fn ui_state(state: &Arc<AppState>) -> Value {
    let (prompt, reasoning) = match state.ui.try_read() {
        Ok(u) => (u.prompt.clone(), u.reasoning),
        Err(_) => (None, state.cfg.resolve_show_reasoning()),
    };
    let mode = state
        .permission
        .as_ref()
        .map(|p| {
            p.lock()
                .unwrap_or_else(|e| e.into_inner())
                .mode()
                .to_string()
        })
        .unwrap_or_else(|| "standard".to_string());
    let mut prompts: Vec<String> = state.context.prompts.keys().cloned().collect();
    prompts.sort();
    let cwd = std::env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();
    json!({
        "type": "ui_state",
        "prompt": prompt,
        "prompts": prompts,
        "mode": mode,
        "modes": ["standard", "guarded", "readonly", "restrictive", "yolo"],
        "reasoning": reasoning,
        "cwd": cwd,
    })
}

/// Begin a turn (shared by `prompt` and `retry_turn`).
async fn begin_turn<F, Fut>(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    turn: &mut Option<Turn>,
    send: &F,
    ctl_tx: &mpsc::Sender<ConnMsg>,
    text: String,
) where
    F: Fn(Value) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    if turn.is_some()
        || conn
            .busy
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
    {
        send(json!({"type": "error", "message": "a turn is already running"})).await;
        return;
    }
    match start_turn(state, conn, text).await {
        Ok((rx, abort, user_msg_len)) => {
            let fwd = tokio::spawn(forward_runner(rx, ctl_tx.clone()));
            *turn = Some(Turn {
                abort,
                fwd,
                user_msg_len,
                pending_asks: Vec::new(),
            });
        }
        Err(e) => {
            conn.busy.store(false, Ordering::SeqCst);
            send(json!({"type": "error", "message": e.to_string()})).await;
        }
    }
}

/// Prompt for the next `/loop` iteration, with the plan file inline.
fn loop_iteration_prompt(task: &str, iteration: u32, max: u32) -> String {
    let plan = std::fs::read_to_string("LOOP_PLAN.md").unwrap_or_else(|_| "(尚未创建)".into());
    let plan: String = plan.chars().take(6000).collect();
    format!(
        "【循环任务 {iteration}/{max}】\n{task}\n\n当前计划文件 LOOP_PLAN.md:\n```\n{plan}\n```\n\n\
         请：1) 从计划中挑一个未完成项执行；2) 用 write/edit 更新 LOOP_PLAN.md（勾掉已完成项）；\
         3) 用一两句话总结本轮做了什么。"
    )
}

async fn loop_status(conn: &Arc<Conn>) -> Value {
    let g = conn.loop_run.lock().await;
    match g.as_ref() {
        Some(l) => json!({
            "type": "loop_status",
            "active": true,
            "iteration": l.iteration,
            "max": l.max,
            "prompt": l.prompt,
        }),
        None => json!({"type": "loop_status", "active": false}),
    }
}

/// Drive the next loop iteration once a turn has finished.
async fn maybe_next_loop<F, Fut>(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    turn: &mut Option<Turn>,
    send: &F,
    ctl_tx: &mpsc::Sender<ConnMsg>,
) where
    F: Fn(Value) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let next = {
        let mut g = conn.loop_run.lock().await;
        match g.as_mut() {
            Some(l) if l.iteration < l.max => {
                l.iteration += 1;
                Some(loop_iteration_prompt(&l.prompt, l.iteration, l.max))
            }
            Some(_) => {
                *g = None;
                None
            }
            None => None,
        }
    };
    send(loop_status(conn).await).await;
    if let Some(text) = next {
        begin_turn(state, conn, turn, send, ctl_tx, text).await;
    }
}

/// Providers + quick models for the settings dialog.
async fn config_payload(state: &Arc<AppState>) -> Value {
    let mut models: Vec<Value> = Vec::new();
    let overlay = state.models_overlay.read().await.clone();
    let overlaid: Vec<&str> = overlay.iter().map(|(n, _, _)| n.as_str()).collect();
    let qm = crate::config::quick_models_map_ref(&state.cfg);
    let mut names: Vec<&String> = qm.keys().filter(|n| !overlaid.contains(&n.as_str())).collect();
    names.sort();
    for name in names {
        let q = &qm[name];
        models.push(json!({
            "name": name,
            "provider": q.provider,
            "model": q.model,
            "source": "config",
        }));
    }
    for (name, provider, model) in overlay.iter() {
        models.push(json!({
            "name": name,
            "provider": provider,
            "model": model,
            "source": "ui",
        }));
    }
    let mut providers: Vec<Value> = vec![
        json!({"name": "openrouter", "kind": "builtin"}),
        json!({"name": "openai", "kind": "builtin"}),
        json!({"name": "anthropic", "kind": "builtin"}),
        json!({"name": "gemini", "kind": "builtin"}),
        json!({"name": "ollama", "kind": "builtin"}),
    ];
    for (name, p) in state.cfg.custom_providers_map() {
        providers.push(json!({
            "name": name,
            "kind": "custom",
            "base_url": p.base_url.clone(),
            "api_key_env": p.api_key_env.clone(),
        }));
    }
    json!({
        "type": "config_state",
        "providers": providers,
        "models": models,
        "config_file": crate::config::config_file_path().to_string_lossy().to_string(),
    })
}

/// Current attachment list for one connection.
async fn attached_payload(conn: &Arc<Conn>) -> Value {
    json!({"type": "attached", "files": conn.attached.lock().await.clone()})
}
