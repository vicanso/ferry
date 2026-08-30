//! 合并入口:bridge-agent 与 git-patch 服务跑在**同一个进程**里。
//!
//! # 为什么同进程
//!
//! git-patch 服务本身就是个 HTTP 服务,而 agent 的职责正是「把本地 HTTP 服务经
//! Redis 暴露出去」。两者同进程后,agent 用 `127.0.0.1` 就能直连它 —— 分开成两个
//! 容器时回环地址不通,还得额外规划容器网络。于是调用方只跟 Redis 打交道:
//!
//! ```text
//! A 侧 bridge-client
//!   → LPUSH bridge:req:{service}
//!   → agent BRPOP → http://gitpatch/patch?... → 127.0.0.1 上的本服务
//!   → 响应原路写回 Redis
//! ```
//!
//! 对应配置(把本服务注册成一个 upstream):
//!
//! ```text
//! FERRY__SERVER__ROOT=/srv/repos
//! FERRY__AGENT__UPSTREAMS__GITPATCH__BASE=http://127.0.0.1:7100
//! ```
//!
//! # 两个子系统各自可选
//!
//! 谁配了就跑谁:`agent.upstreams` 为空则只跑 git-patch 服务,`server.root` 为空
//! 则只跑 agent,都没配就拒绝启动。**原有的纯 agent 部署换成本二进制无需改任何
//! 环境变量** —— 没配 `server.root`,git-patch 服务就是关着的。
//!
//! # 关于体积上限
//!
//! 经 Redis 取 patch 时,响应要过 bridge 协议的 4 MiB 上限,而 patch 放进 JSON 会
//! 因转义而膨胀(每个换行 1 字节变 2)。所以 `server.max_patch_bytes` 默认取 2 MiB,
//! 留足余量;详见 git-api 的 default.toml。

use snafu::{prelude::*, Report};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Snafu)]
enum FerryError {
    #[snafu(display("bridge agent failed"))]
    Agent { source: bridge_agent::AgentError },
    #[snafu(display("git patch api failed"))]
    Api { source: ferry_git_api::StartupError },
    #[snafu(display(
        "nothing to run: set agent.upstreams (FERRY__AGENT__UPSTREAMS__*) \
         or server.root (FERRY__SERVER__ROOT), or both"
    ))]
    NothingConfigured,
}

#[tokio::main]
async fn main() -> Report<FerryError> {
    run().await.into()
}

// AgentError 有 128+ 字节(多个变体带 String + source),但它只在启动路径上返回
// 一次,不在任何热路径里;为它加一层 Box 只会让错误处理更绕。
#[allow(clippy::result_large_err)]
async fn run() -> Result<(), FerryError> {
    // 全进程只装一次 subscriber —— 两个子系统的库都刻意不碰它
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // 「没配」与「配错了」必须分开:前者是关掉该子系统,后者要立刻失败。
    // 两个库的 load_config 各自用一个专门的错误变体表达「没配」。
    let agent_cfg = match bridge_agent::load_config() {
        Ok(cfg) => Some(cfg),
        Err(bridge_agent::AgentError::EmptyUpstreams) => {
            tracing::info!("bridge agent disabled (agent.upstreams is empty)");
            None
        }
        Err(source) => return Err(FerryError::Agent { source }),
    };
    let api_cfg = match ferry_git_api::load_config() {
        Ok(cfg) => Some(cfg),
        Err(ferry_git_api::StartupError::EmptyRoot) => {
            tracing::info!("git patch api disabled (server.root is empty)");
            None
        }
        Err(source) => return Err(FerryError::Api { source }),
    };
    ensure!(
        agent_cfg.is_some() || api_cfg.is_some(),
        NothingConfiguredSnafu
    );

    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            bridge_agent::shutdown_signal().await;
            tracing::info!("shutdown signal received; draining both subsystems");
            shutdown.cancel();
        });
    }

    // 每个子系统退出时都 cancel 一次:任一半边倒下(比如端口被占、Redis 连不上),
    // 另一半也收摊,进程整体退出并带上错误码 —— 免得容器"活着但只剩一半功能"。
    let agent = {
        let shutdown = shutdown.clone();
        async move {
            let result = match agent_cfg {
                Some(cfg) => bridge_agent::run(cfg, shutdown.clone())
                    .await
                    .context(AgentSnafu),
                // 没启用:挂在关闭信号上而不是永久 pending,否则下面的 join 收不了尾
                None => {
                    shutdown.cancelled().await;
                    Ok(())
                }
            };
            shutdown.cancel();
            result
        }
    };
    let api = {
        let shutdown = shutdown.clone();
        async move {
            let result = match api_cfg {
                Some(cfg) => ferry_git_api::serve(cfg, shutdown.clone())
                    .await
                    .context(ApiSnafu),
                None => {
                    shutdown.cancelled().await;
                    Ok(())
                }
            };
            shutdown.cancel();
            result
        }
    };

    let (agent_result, api_result) = tokio::join!(agent, api);
    // agent 的错误优先冒泡:它是主业务,api 多半只是被连带关掉
    agent_result.and(api_result)
}
