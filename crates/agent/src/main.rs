//! `bridge-agent` 的独立入口:装日志、接信号,其余全在库里。
//!
//! 想让 agent 和 git-patch 服务跑在同一个进程里,用 `ferry` 那个二进制,不要同时
//! 起这个 —— 两个进程各自 BRPOP 同一条队列会互相抢请求。

use bridge_agent::AgentError;
use snafu::Report;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Report<AgentError> {
    run().await.into()
}

// AgentError 有 128+ 字节(多个变体带 String + source),但它只在启动路径上返回
// 一次,不在任何热路径里;为它加一层 Box 只会让错误处理更绕。
#[allow(clippy::result_large_err)]
async fn run() -> Result<(), AgentError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = bridge_agent::load_config()?;

    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            bridge_agent::shutdown_signal().await;
            tracing::info!("shutdown signal received; stop pulling, draining in-flight");
            shutdown.cancel();
        });
    }

    bridge_agent::run(cfg, shutdown).await
}
