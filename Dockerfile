# ── Stage 1: 构建 ─────────────────────────────────────────────
FROM rust:1.96-bookworm AS builder

# ⚠️ 项目 .cargo/config.toml 强制 mold 链接器（fuse-ld=mold），
#    不装 mold 构建必然失败——这是本地编译时踩过的坑。
RUN apt-get update \
    && apt-get install -y --no-install-recommends mold git pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

# 先拉依赖，充分利用 Docker 层缓存（源码改动不重新下载 crates）
RUN cargo fetch
# web = Web UI；multithread = 多核 tokio runtime（默认是单线程，服务端建议开）
RUN cargo build --release --features web,multithread \
    && strip target/release/zerostack

# ── Stage 2: 运行时（单二进制 + git，无 Rust 工具链）──────────
# Web UI 的前端资源通过 include_str! 编译进二进制，无需额外资产文件
FROM debian:bookworm-slim

# git：agent 的 git 操作（worktree/refresh_git_branch）需要；
# curl：仅用于 HEALTHCHECK
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 zerostack

COPY --from=builder /build/target/release/zerostack /usr/local/bin/zerostack

# 非 root 运行：agent 会在容器内执行 bash 命令，降权是第一道边界
USER zerostack
WORKDIR /workspace

# 配置与会话目录：全部挂卷，镜像无状态、可随时升级
ENV ZS_CONFIG_DIR=/data/config \
    ZS_DATA_DIR=/data/sessions
VOLUME ["/data", "/workspace"]

EXPOSE 18800
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD curl -sf http://127.0.0.1:18800/ || exit 1

ENTRYPOINT ["zerostack"]
CMD ["--web", "--web-host", "0.0.0.0"]
