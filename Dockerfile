# syntax=docker/dockerfile:1

# 构建产物是 bridge-agent(B 侧)。bridge-client 是库,由调用方自己集成,
# 不产出二进制,因此这里只构建 agent。

ARG RUST_VERSION=1
ARG DEBIAN_RELEASE=bookworm

# ---------------------------------------------------------------------------
# 构建阶段
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder

WORKDIR /build

# 先只复制清单,把「编译依赖」固化成独立的一层。之后只改业务代码时这层命中
# 缓存,不必重新编译几百个依赖 —— 这是多阶段构建在 Rust 上最主要的收益。
COPY Cargo.toml Cargo.lock ./
COPY crates/protocol/Cargo.toml crates/protocol/
COPY crates/client/Cargo.toml   crates/client/
COPY crates/agent/Cargo.toml    crates/agent/

# workspace 的每个成员都必须有源码文件,cargo 才肯解析清单,先放空壳。
# client 虽然不参与 -p bridge-agent 的构建,但缺了它 workspace 解析会失败。
RUN mkdir -p crates/protocol/src crates/client/src crates/agent/src \
    && : > crates/protocol/src/lib.rs \
    && : > crates/client/src/lib.rs \
    && echo 'fn main() {}' > crates/agent/src/main.rs \
    && cargo build --release --locked -p bridge-agent

COPY crates crates

# COPY 保留构建上下文里的 mtime,可能比上一层空壳编译的产物还旧,cargo 会据此
# 误判「无需重建」,最后打包进镜像的就是那个空壳二进制。touch 一遍消除歧义。
# (这个坑在本项目开发期间真实踩到过两次,只是换成了本地构建的形式。)
RUN find crates -name '*.rs' -exec touch {} + \
    && cargo build --release --locked -p bridge-agent \
    && strip target/release/bridge-agent

# ---------------------------------------------------------------------------
# 运行阶段
# ---------------------------------------------------------------------------
# 与 builder 用同一个 Debian 版本,保证 glibc 兼容
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# 不装 ca-certificates:当前没有任何 TLS 依赖(reqwest 未启用 TLS feature,
# upstream 是本机明文 HTTP;Redis 也未启用 TLS)。
#
# 一旦要用 TLS(upstream 走 https,或 Redis 走 rediss://),统一用 rustls,
# 不要用 native-tls —— 那会把系统 openssl 拖进构建阶段。届时这一行要不要改,
# 取决于根证书从哪来:
#   - webpki-roots(reqwest 的 "rustls-tls"):根证书编译进二进制,这里无需改动
#   - 系统信任库(reqwest 的 "rustls-tls-native-roots",以及 redis 默认的
#     "tls-rustls",它走 rustls-native-certs):必须在这里装 ca-certificates,
#     否则握手会以「找不到任何根证书」失败
# rustls 在 redis 0.27 下锁定 ring 作为 crypto provider(不是 aws-lc-rs),
# 构建阶段不需要额外装 cmake / C 编译器。
RUN useradd --system --create-home --uid 10001 app

# COPY 必须在 USER 之前:写入 /usr/local/bin 需要 root。docker 与
# podman/buildah 对「COPY 是否受 USER 影响」的处理并不一致,顺序摆对就没有歧义。
# 二进制保持 root 所有、app 只读可执行,应用自身无法改写它。
COPY --from=builder /build/target/release/bridge-agent /usr/local/bin/bridge-agent

USER app
WORKDIR /home/app

# 默认配置已烘焙进二进制(crates/agent/config/default.toml),容器里不必挂文件。
# 环境变量前缀 FERRY,层级分隔符是双下划线,优先级高于所有 TOML 源。
# 需要复杂配置时挂一个 TOML 进来并用 FERRY_CONFIG 指向它即可。
#
# upstreams 是「服务名 → 真实上游」的映射,调用方只给服务名,真实地址(及可选的注入
# header)只在这里。无 header 用逗号分隔的整表简写(如下);要给某服务注入 header
# (如统一下发 Authorization,免得密钥进 Redis)用嵌套写法:
#   -e FERRY__AGENT__UPSTREAMS__GROK__BASE=http://... \
#   -e FERRY__AGENT__UPSTREAMS__GROK__HEADERS__AUTHORIZATION='Bearer ...'
# 留空会拒绝启动 —— 必须显式指定。这里给个占位默认,部署时按实际服务覆盖。
ENV FERRY__AGENT__UPSTREAMS=demo=http://127.0.0.1:8080 \
    RUST_LOG=info

# agent 以 PID 1 运行并自行处理 SIGTERM:收到后停止拉取新请求,等 in-flight
# 全部完成再退出。编排器的 grace period 要留够(docker stop 默认 10s,
# k8s 的 terminationGracePeriodSeconds 默认 30s),否则排空到一半会被 SIGKILL。
STOPSIGNAL SIGTERM

# 没有 HEALTHCHECK:agent 不监听任何端口,健康状况看日志里的 backlog 指标
# (队列积压是最重要的信号,一涨就说明处理不过来或已挂掉)。

ENTRYPOINT ["/usr/local/bin/bridge-agent"]
