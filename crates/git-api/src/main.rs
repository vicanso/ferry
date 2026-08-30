//! `ferry-git-api` 的独立入口:装日志、接信号,其余全在库里。
//!
//! 要和 bridge-agent 同进程跑,用 `ferry` 那个二进制。

use ferry_git_api::StartupError;
use snafu::Report;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Report<StartupError> {
    run().await.into()
}

async fn run() -> Result<(), StartupError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = ferry_git_api::load_config()?;

    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            tracing::info!("shutdown signal received; draining");
            shutdown.cancel();
        });
    }

    ferry_git_api::serve(cfg, shutdown).await
}

/// 必须同时监听 SIGTERM:编排器发的是它,只听 SIGINT 会在 grace period 后被强杀。
async fn shutdown_signal() {
    let ctrl_c = async { tokio::signal::ctrl_c().await.ok() };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "cannot install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!(signal = "SIGINT", "shutting down"),
        _ = term => tracing::info!(signal = "SIGTERM", "shutting down"),
    }
}
