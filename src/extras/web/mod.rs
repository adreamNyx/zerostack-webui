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
//!   `{"type":"session_list"}`                           refresh sidebar list
//!   `{"type":"switch_model","name":"<quick-model>"}`    hot-swap provider/model
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
use axum::routing::get;
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
use crate::provider::{build_agent, AnyClient};
use crate::sandbox::Sandbox;
use crate::session::storage;
use crate::session::{MessageRole, Session};
use crate::ui::state::TurnUsage;

const INDEX_HTML: &str = include_str!("static/index.html");
const APP_JS: &str = include_str!("static/app.js");
const STYLE_CSS: &str = include_str!("static/style.css");

const DEFAULT_PORT: u16 = 8629;

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

struct AppState {
    cli: Cli,
    cfg: Config,
    context: ContextFiles,
    runtime: AsyncRwLock<RuntimeModel>,
    permission: Option<PermCheck>,
    ask_tx: Option<AskSender>,
    sandbox: Sandbox,
    session: AsyncMutex<Session>,
    /// One agent turn at a time across all connections (same semantics as a
    /// single TUI instance).
    busy: AtomicBool,
    /// AskRequest proxy broadcast (hub → connections).
    ask_bc: broadcast::Sender<Arc<PermAsk>>,
    /// Connection → hub decisions.
    decisions_tx: mpsc::Sender<Decision>,
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

    let state = Arc::new(AppState {
        cli,
        cfg,
        context,
        runtime: AsyncRwLock::new(RuntimeModel {
            client,
            provider,
            model,
        }),
        permission,
        ask_tx,
        sandbox,
        session: AsyncMutex::new(session),
        busy: AtomicBool::new(false),
        ask_bc,
        decisions_tx: dec_tx,
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/ws", get(ws_handler))
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

    let mut turn: Option<Turn> = None;
    let mut pending_calls: Vec<(CompactString, u64)> = Vec::new();
    let mut ask_sub = state.ask_bc.subscribe();

    // Initial payload: full session so the client can render history.
    {
        let session = state.session.lock().await;
        send(session_state(&session, &state)).await;
    }

    loop {
        tokio::select! {
            ctl = ctl_rx.recv() => {
                let Some(msg) = ctl else { break };
                match msg {
                    ConnMsg::Inbound(v) => {
                        handle_inbound(&state, v, &mut turn, &send, &ctl_tx).await;
                    }
                    ConnMsg::Agent(event) => {
                        let done = if let Some(t) = turn.as_mut() {
                            handle_event(&state, t, event, &mut pending_calls, &send).await
                        } else {
                            false
                        };
                        if done {
                            retire_turn(&state, &mut turn).await;
                        }
                    }
                    ConnMsg::AgentClosed => {
                        if let Some(t) = turn.take() {
                            t.abort.abort();
                            t.fwd.abort();
                            deny_pending(&state, &t.pending_asks).await;
                            state.busy.store(false, Ordering::SeqCst);
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
        state.busy.store(false, Ordering::SeqCst);
    }
    reader.abort();
    forwarder.abort();
}

/// Handle one inbound browser message. Turn completion is signalled by
/// `handle_event` (Done/Error), never here.
async fn handle_inbound<F, Fut>(
    state: &Arc<AppState>,
    v: Value,
    turn: &mut Option<Turn>,
    send: &F,
    ctl_tx: &mpsc::Sender<ConnMsg>,
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
            if turn.is_some()
                || state
                    .busy
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                send(json!({"type": "error", "message": "a turn is already running"})).await;
                return;
            }
            match start_turn(state, text.to_string()).await {
                Ok((rx, abort, user_msg_len)) => {
                    // Forward runner events into the control channel so the
                    // main loop never holds two receiving ends at once.
                    let fwd = tokio::spawn(forward_runner(rx, ctl_tx.clone()));
                    *turn = Some(Turn {
                        abort,
                        fwd,
                        user_msg_len,
                        pending_asks: Vec::new(),
                    });
                }
                Err(e) => {
                    state.busy.store(false, Ordering::SeqCst);
                    send(json!({"type": "error", "message": e.to_string()})).await;
                }
            }
        }
        Some("cancel") => {
            if let Some(t) = turn.take() {
                t.abort.abort();
                t.fwd.abort();
                deny_pending(state, &t.pending_asks).await;
                state.busy.store(false, Ordering::SeqCst);
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
            let s = new_session(state).await;
            let payload = session_state(&s, state);
            *state.session.lock().await = s;
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
                    *state.session.lock().await = s;
                    send(payload).await;
                }
                Ok(_) => {
                    send(json!({"type": "error", "message": format!("session not found: {id}")}))
                        .await;
                }
                Err(e) => send(json!({"type": "error", "message": e.to_string()})).await,
            }
        }
        Some("session_list") => {
            let sessions = storage::find_recent_sessions(30)
                .map(|list| list.iter().map(session_summary).collect::<Vec<_>>())
                .unwrap_or_default();
            send(json!({"type": "session_list", "sessions": sessions})).await;
        }
        Some("switch_model") => {
            let Some(name) = v.get("name").and_then(Value::as_str) else {
                return;
            };
            let qm = crate::config::quick_models_map_ref(&state.cfg);
            let Some(q) = qm.get(name) else {
                send(json!({"type": "error", "message": format!("unknown model: {name}")})).await;
                return;
            };
            // Rebuild the client for the target provider, then hot-swap the
            // runtime triple. A turn already in flight keeps its own model;
            // the next prompt picks this one up.
            match crate::provider::create_client(
                q.provider.as_str(),
                state.cli.api_key.as_deref(),
                &state.cfg.custom_providers_map(),
                state.cfg.api_keys.as_ref(),
            ) {
                Ok(client) => {
                    {
                        let mut rt = state.runtime.write().await;
                        rt.client = client;
                        rt.provider = q.provider.to_string();
                        rt.model = q.model.to_string();
                    }
                    {
                        let mut session = state.session.lock().await;
                        session.provider = q.provider.clone();
                        session.model = q.model.clone();
                    }
                    let session = state.session.lock().await;
                    send(session_state(&session, state)).await;
                }
                Err(e) => send(json!({"type": "error", "message": e.to_string()})).await,
            }
        }
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
async fn retire_turn(state: &Arc<AppState>, turn: &mut Option<Turn>) {
    if let Some(t) = turn.take() {
        t.fwd.abort();
        deny_pending(state, &t.pending_asks).await;
        state.busy.store(false, Ordering::SeqCst);
    }
}

/// Returns `true` when the turn is over (Done or Error).
async fn handle_event<F, Fut>(
    state: &Arc<AppState>,
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
            if state.cfg.resolve_show_reasoning() {
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
            let mut session = state.session.lock().await;
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
            let mut session = state.session.lock().await;
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
            let mut session = state.session.lock().await;
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
            let mut session = state.session.lock().await;
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
            let mut session = state.session.lock().await;
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
            let mut session = state.session.lock().await;
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

    let agent = build_agent(
        model,
        AgentBuild {
            cli: &state.cli,
            cfg: &state.cfg,
            context: &state.context,
            permission: state.permission.clone(),
            ask_tx: state.ask_tx.clone(),
            sandbox: state.sandbox.clone(),
            reasoning_enabled: false,
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
        let mut session = state.session.lock().await;
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

async fn new_session(state: &Arc<AppState>) -> Session {
    let old = state.session.lock().await;
    save_best_effort(&state.cli, &old);
    Session::new(&old.provider, &old.model, old.context_window, "")
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
