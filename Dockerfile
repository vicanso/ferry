# syntax=docker/dockerfile:1

# 构建产物是 ferry:agent 与 git-patch 服务同进程的合并二进制。
# 两个子系统按配置各自启用(agent.upstreams / server.root),都不配则拒绝启动;
# 原有的纯 agent 部署直接换成它即可,环境变量一个都不用改。
# bridge-client 是库,由调用方自己集成,不产出二进制。

ARG RUST_VERSION=1
ARG DEBIAN_RELEASE=bookworm

# ---------------------------------------------------------------------------
# 构建阶段
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder

# git2 走 libgit2,它的 https/ssh 传输链的是系统 openssl 与 libssh2 —— libgit2 是
# C 库,不认 rustls,这是全项目唯一让 openssl 进来的地方。openssl-sys 的 build 脚本
# 要靠 pkg-config 找头文件,rust 官方镜像里没带 libssl-dev。
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# 先只复制清单,把「编译依赖」固化成独立的一层。之后只改业务代码时这层命中
# 缓存,不必重新编译几百个依赖 —— 这是多阶段构建在 Rust 上最主要的收益。
COPY Cargo.toml Cargo.lock ./
COPY crates/protocol/Cargo.toml crates/protocol/
COPY crates/redis/Cargo.toml    crates/redis/
COPY crates/client/Cargo.toml   crates/client/
COPY crates/agent/Cargo.toml    crates/agent/
COPY crates/git/Cargo.toml      crates/git/
COPY crates/git-api/Cargo.toml  crates/git-api/
COPY crates/ferry/Cargo.toml    crates/ferry/

# workspace 的每个成员都必须有源码文件,cargo 才肯解析整个 workspace,先放空壳 ——
# 少任何一个成员(即便只 -p bridge-agent、client 不参与构建)解析都会失败。
# agent 依赖 protocol 和 redis,它们的空壳 lib 也一并放上。
# agent / git-api 都已拆成 lib + 薄 bin,ferry 依赖两者的库;每个成员的空壳都要
# 按其 target 形状放齐(带 lib 的放 lib.rs,带 bin 的放 main.rs)。
RUN mkdir -p crates/protocol/src crates/redis/src crates/client/src crates/agent/src crates/git/src crates/git-api/src crates/ferry/src \
    && : > crates/protocol/src/lib.rs \
    && : > crates/redis/src/lib.rs \
    && : > crates/client/src/lib.rs \
    && : > crates/git/src/lib.rs \
    && : > crates/agent/src/lib.rs \
    && : > crates/git-api/src/lib.rs \
    && echo 'fn main() {}' > crates/agent/src/main.rs \
    && echo 'fn main() {}' > crates/git/src/main.rs \
    && echo 'fn main() {}' > crates/git-api/src/main.rs \
    && echo 'fn main() {}' > crates/ferry/src/main.rs \
    && cargo build --release --locked -p ferry

COPY crates crates

# COPY 保留构建上下文里的 mtime,可能比上一层空壳编译的产物还旧,cargo 会据此
# 误判「无需重建」,最后打包进镜像的就是那个空壳二进制。touch 一遍消除歧义。
# (这个坑在本项目开发期间真实踩到过两次,只是换成了本地构建的形式。)
RUN find crates -name '*.rs' -exec touch {} + \
    && cargo build --release --locked -p ferry \
    && strip target/release/ferry

# ---------------------------------------------------------------------------
# 运行阶段
# ---------------------------------------------------------------------------
# 与 builder 用同一个 Debian 版本,保证 glibc 兼容
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# libssl3:libgit2 动态链接的 openssl 运行时,由 git-patch 服务引入。只跑 agent
#   (不配 server.root)时用不上,但镜像是同一个,装着无妨。
# ca-certificates:两个子系统都要 —— git fetch 走 https 要校验服务端证书,agent
#   转发 https 上游时 rustls-platform-verifier 读的也是这里的系统信任库。没有它,
#   握手会以「找不到任何根证书」失败。
#   上游或 git remote 若由内网自签 CA 签发(测试环境常见),光装这个包不够:得把那张
#   CA 证书放进 /usr/local/share/ca-certificates/ 再跑一次 update-ca-certificates。
#
# 没装 git:fetch 由 libgit2 自己实现协议,不 fork git 进程。仓库要**预先克隆**
# 到挂载进来的根目录里 —— 那一步在镜像外做。
# ssh 远端另需凭证:libssh2 走 ssh-agent 或密钥文件,容器里通常没有 ssh-agent,
# 建议 remote 用 https + credential helper,或把只读部署密钥挂进来。
#
# agent 的 reqwest 走 rustls + ring provider(见根 Cargo.toml),纯 Rust、不链系统
# openssl,builder 也不需要 cmake/clang —— libssl 只服务于 libgit2 那条线。
# redis 侧仍是明文;要 rediss:// 就在 tibba 侧开 rustls feature。
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 app

# COPY 必须在 USER 之前:写入 /usr/local/bin 需要 root。docker 与
# podman/buildah 对「COPY 是否受 USER 影响」的处理并不一致,顺序摆对就没有歧义。
# 二进制保持 root 所有、app 只读可执行,应用自身无法改写它。
COPY --from=builder /build/target/release/ferry /usr/local/bin/ferry

USER app
WORKDIR /home/app

# 默认配置已烘焙进二进制(agent 与 git-api 各自的 config/default.toml),
# 容器里不必挂文件。git-patch 服务要用时,把仓库根目录挂进来并设 FERRY__SERVER__ROOT;
# 它只绑回环,由 agent 以 upstream 形式经 Redis 对外提供:
#   FERRY__AGENT__UPSTREAMS__GITPATCH__BASE=http://127.0.0.1:7100
# 环境变量前缀 FERRY,层级分隔符是双下划线,优先级高于所有 TOML 源。
# 需要复杂配置时挂一个 TOML 进来并用 FERRY_CONFIG 指向它即可。

ENV RUST_LOG=info

# ferry 以 PID 1 运行并自行处理 SIGTERM:收到后两个子系统一起排空 —— agent 停止
# 拉取新请求、HTTP 服务停止收新连接,各自等在途请求做完再退出。编排器的 grace period 要留够(docker stop 默认 10s,
# k8s 的 terminationGracePeriodSeconds 默认 30s),否则排空到一半会被 SIGKILL。
STOPSIGNAL SIGTERM

# 没有 HEALTHCHECK:只跑 agent 时不监听任何端口,健康状况看日志里的 backlog 指标
# (队列积压是最重要的信号,一涨就说明处理不过来或已挂掉)。配了 git-patch 服务时
# 它有 /health,但那只是回环上的半边,代表不了 agent 是否健在。

ENTRYPOINT ["/usr/local/bin/ferry"]
