# ── 最终镜像：基于预装好依赖的 zsbase（离线构建，无需网络）────
# zsbase 的生成方式（见 docs 或交付说明）：
#   docker run -d --name zsbase ubuntu:24.04 sleep 600
#   docker exec zsbase apt-get update
#   docker exec zsbase apt-get install -y --no-install-recommends git ca-certificates curl tini
#   docker exec zsbase useradd -m -u 1001 zerostack
#   docker commit zsbase zsbase:latest && docker rm -f zsbase
# 受限网络环境下 BuildKit 的 RUN 无法出网，故用 run+exec+commit 绕过。

FROM zsbase:latest

COPY docker/zerostack /usr/local/bin/zerostack
RUN chmod +x /usr/local/bin/zerostack

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

# tini 作为 init：正确转发信号，保证 docker stop 时会话落盘
ENTRYPOINT ["/usr/bin/tini", "--", "zerostack"]
CMD ["--web", "--web-host", "0.0.0.0"]
