//! `ssh` agent tool: run one command on a remote host over SSH.
//!
//! Deliberately shells out to the system `ssh` binary instead of linking an
//! SSH implementation: it honours `~/.ssh/config`, agent forwarding, jump
//! hosts and known-host state exactly like the user's own terminal, and adds
//! no dependencies to the crate.
//!
//! Every invocation is non-interactive by design (the agent has no TTY):
//! `BatchMode=yes` unless a password is supplied through `sshpass`, host keys
//! are accepted on first use (`accept-new`), and a `ControlMaster` socket is
//! reused per host so a multi-step investigation over one host pays the
//! handshake only once.

use std::collections::HashMap;

use rig::tool::PortableTool as Tool;
use serde::Deserialize;
use tokio::time::{Duration, timeout};

use crate::agent::tools::{AskSender, PermCheck, ToolError, check_perm};
use crate::config::types::SshHostConfig;
use crate::extras::truncate::head_lines;

const SSH_DESCRIPTION: &str = r#"Run a shell command on a REMOTE machine over SSH.

Use this instead of `bash` whenever the work has to happen on another host:
inspecting remote logs, restarting remote services, checking disk/CPU on a
server, deploying, reading remote config. The command runs in the remote
user's login shell, non-interactively (`BatchMode`), so anything that needs a
password prompt, a TTY editor (vim/less/top) or `sudo -S` without a configured
NOPASSWD entry will fail — use non-interactive flags (`-y`, `--no-pager`,
`systemctl --no-pager`) instead.

Host resolution:
  1. `host` naming a key under `[ssh_hosts.<name>]` in the config uses that
     entry (host/user/port/identity_file/password_env).
  2. otherwise `host` may be a literal `user@host` or a `~/.ssh/config` alias.

Each call is independent: no cwd/env persistence between calls, so chain with
`&&` or use an absolute path. A `ControlMaster` socket is kept per host for
5 minutes, which makes repeated calls cheap.

Prefer read-only commands. Anything that mutates the remote host needs the
same care you would apply by hand: check first, back up second, change third."#;

/// One remote host, either from `[ssh_hosts.<name>]` or inline in the call.
#[derive(Deserialize)]
pub struct SshArgs {
    /// Host alias from `[ssh_hosts]`, or `user@host`, or a `~/.ssh/config` alias.
    pub host: String,
    /// Shell command to run remotely.
    pub command: String,
    /// Overrides the configured user.
    pub user: Option<String>,
    /// Overrides the configured port.
    pub port: Option<u16>,
    /// Overrides the configured private key path.
    pub identity_file: Option<String>,
    /// Env var holding the password (falls back to `sshpass -e`).
    pub password_env: Option<String>,
    /// Timeout in milliseconds; the SSH connection itself uses `connect_timeout`.
    pub timeout: Option<u64>,
    /// Connection timeout in seconds (default 10).
    pub connect_timeout: Option<u64>,
    /// Allocate a remote TTY (`-tt`). Only useful for commands that need one;
    /// it never makes the tool interactive.
    pub tty: Option<bool>,
}

pub struct SshTool {
    pub permission: Option<PermCheck>,
    pub ask_tx: Option<AskSender>,
    /// `[ssh_hosts]` entries from the config.
    pub hosts: HashMap<String, SshHostConfig>,
    /// `None` = no truncation; `Some(n)` = head-only after `n` lines.
    pub max_output_lines: Option<u64>,
}

impl SshTool {
    pub fn new(
        permission: Option<PermCheck>,
        ask_tx: Option<AskSender>,
        hosts: HashMap<String, SshHostConfig>,
        max_output_lines: Option<u64>,
    ) -> Self {
        SshTool {
            permission,
            ask_tx,
            hosts,
            max_output_lines,
        }
    }
}

/// What one call resolved to before touching the network.
struct Target {
    host: String,
    user: Option<String>,
    port: Option<u16>,
    identity_file: Option<String>,
    password_env: Option<String>,
}

impl SshTool {
    fn resolve(&self, args: &SshArgs) -> Target {
        let alias = self.hosts.get(&args.host);
        let base = alias.cloned().unwrap_or_default();
        // `user@host` in the call itself wins over anything configured.
        let (inline_user, inline_host) = match args.host.split_once('@') {
            Some((u, h)) if !u.is_empty() && !h.is_empty() => (Some(u.to_string()), h.to_string()),
            _ => (None, base.host.clone().if_empty_then(&args.host)),
        };
        Target {
            host: inline_host,
            user: args
                .user
                .clone()
                .or(inline_user)
                .or(base.user.clone()),
            port: args.port.or(base.port),
            identity_file: args
                .identity_file
                .clone()
                .or(base.identity_file.clone()),
            password_env: args
                .password_env
                .clone()
                .or(base.password_env.clone()),
        }
    }
}

trait IfEmptyThen {
    fn if_empty_then(self, fallback: &str) -> String;
}
impl IfEmptyThen for String {
    fn if_empty_then(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

impl Tool for SshTool {
    const NAME: &'static str = "ssh";

    type Error = ToolError;
    type Args = SshArgs;
    type Output = String;

    fn description(&self) -> String {
        let mut d = SSH_DESCRIPTION.to_string();
        if !self.hosts.is_empty() {
            let mut names: Vec<&String> = self.hosts.keys().collect();
            names.sort();
            d.push_str("\n\nConfigured hosts:\n");
            for n in names {
                let h = &self.hosts[n];
                let mut line = format!(
                    "- `{n}` → {}@{}",
                    h.user.clone().unwrap_or_else(|| "?".into()),
                    h.host
                );
                if let Some(p) = h.port {
                    line.push_str(&format!(":{p}"));
                }
                if let Some(desc) = &h.description {
                    line.push_str(&format!(" — {desc}"));
                }
                d.push_str(&line);
                d.push('\n');
            }
        }
        d
    }

    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                    "description": "Host alias from [ssh_hosts], or user@host, or a ~/.ssh/config alias"
                },
                "command": { "type": "string", "description": "Shell command to run on the remote host" },
                "user": { "type": "string", "description": "SSH user (optional)" },
                "port": { "type": "integer", "description": "SSH port (optional)" },
                "identity_file": { "type": "string", "description": "Private key path (optional)" },
                "password_env": { "type": "string", "description": "Env var holding the password (optional; needs sshpass)" },
                "timeout": { "type": "integer", "description": "Timeout in milliseconds (optional)" },
                "connect_timeout": { "type": "integer", "description": "Connection timeout in seconds (default 10)" },
                "tty": { "type": "boolean", "description": "Allocate a remote TTY (-tt); still non-interactive" }
            },
            "required": ["host", "command"]
        })
    }

    async fn call(&self, args: SshArgs) -> Result<String, ToolError> {
        if args.command.trim().is_empty() {
            return Err(ToolError::Msg("command must not be empty".to_string()));
        }
        let target = self.resolve(&args);
        if target.host.trim().is_empty() {
            return Err(ToolError::Msg("host must not be empty".to_string()));
        }

        // Permission gate: mirror `bash`, keyed on the human-readable target.
        let perm_key = format!(
            "{}{} {}",
            target
                .user
                .as_ref()
                .map(|u| format!("{u}@"))
                .unwrap_or_default(),
            target.host,
            args.command
        );
        let coaching = check_perm(&self.permission, &self.ask_tx, "ssh", &perm_key).await?;

        let password = target
            .password_env
            .as_ref()
            .and_then(|var| std::env::var(var).ok())
            .filter(|p| !p.is_empty());
        let has_sshpass = std::process::Command::new("sshpass")
            .arg("-V")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if password.is_some() && !has_sshpass {
            return Err(ToolError::Msg(
                "password_env is set but `sshpass` is not installed on this machine; install it or use a key (identity_file)".to_string(),
            ));
        }

        let mut cmd = if password.is_some() {
            let mut c = tokio::process::Command::new("sshpass");
            c.arg("-e");
            c.arg("ssh");
            c
        } else {
            tokio::process::Command::new("ssh")
        };

        cmd.arg("-o").arg("StrictHostKeyChecking=accept-new");
        cmd.arg("-o")
            .arg(format!("ConnectTimeout={}", args.connect_timeout.unwrap_or(10)));
        cmd.arg("-o").arg("ServerAliveInterval=15");
        cmd.arg("-o").arg("ServerAliveCountMax=4");
        // Reuse one connection per host: the agent makes many small calls.
        cmd.arg("-o").arg("ControlMaster=auto");
        cmd.arg("-o")
            .arg("ControlPath=/tmp/zerostack-ssh-%r@%h:%p");
        cmd.arg("-o").arg("ControlPersist=300");
        if password.is_some() {
            // sshpass feeds the password over the TTY, so BatchMode must stay
            // off, but nothing else may prompt.
            cmd.arg("-o").arg("NumberOfPasswordPrompts=1");
        } else {
            cmd.arg("-o").arg("BatchMode=yes");
        }
        if args.tty.unwrap_or(false) {
            cmd.arg("-tt");
        } else {
            cmd.arg("-T");
        }
        if let Some(port) = target.port {
            cmd.arg("-p").arg(port.to_string());
        }
        if let Some(key) = &target.identity_file {
            cmd.arg("-i").arg(expand_home(key));
        }
        let dest = match &target.user {
            Some(u) => format!("{u}@{}", target.host),
            None => target.host.clone(),
        };
        cmd.arg(dest);
        cmd.arg("--");
        cmd.arg(&args.command);
        cmd.stdin(std::process::Stdio::null());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        if let Some(p) = password {
            cmd.env("SSHPASS", p);
        }
        cmd.kill_on_drop(true);

        tracing::debug!(
            "tool ssh: host={} cmd_len={} tty={}",
            target.host,
            args.command.len(),
            args.tty.unwrap_or(false)
        );

        let output = match args.timeout {
            Some(ms) => match timeout(Duration::from_millis(ms), cmd.output()).await {
                Ok(res) => res.map_err(|e| ToolError::Msg(format!("ssh failed to start: {e}")))?,
                Err(_) => {
                    return Err(ToolError::Msg(format!(
                        "ssh command timed out after {ms} ms"
                    )));
                }
            },
            None => cmd
                .output()
                .await
                .map_err(|e| ToolError::Msg(format!("ssh failed to start: {e}")))?,
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let code = output.status.code().unwrap_or(-1);

        let mut result = String::new();
        if !stdout.trim().is_empty() {
            result.push_str(&stdout);
        }
        if !stderr.trim().is_empty() {
            if !result.is_empty() && !result.ends_with('\n') {
                result.push('\n');
            }
            result.push_str("[stderr]\n");
            result.push_str(&stderr);
        }
        if result.trim().is_empty() {
            result.push_str("(no output)");
        }
        if code != 0 {
            result.push_str(&format!("\nExit code: {code}"));
            if code == 255 {
                result.push_str(
                    "\n(255 usually means the SSH connection itself failed: unreachable host, \
                     refused port, or authentication rejected.)",
                );
            }
        }

        let mut result = match self.max_output_lines {
            Some(cap) => {
                let cap = cap as usize;
                let (head, total) = head_lines(&result, cap);
                if total > cap {
                    format!(
                        "{head}\n\n[truncated after {cap} lines — {} more lines elided; \
                         re-run with a narrower command (pipe through tail/grep)]",
                        total - cap
                    )
                } else {
                    result
                }
            }
            None => result,
        };
        if let Some(msg) = coaching {
            result = format!("{msg}\n\n{result}");
        }
        Ok(result)
    }
}

/// `~/x` → `/home/you/x`: the key path comes from config and may be written
/// with a tilde.
fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return format!("{home}/{rest}");
    }
    path.to_string()
}
