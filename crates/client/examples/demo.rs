//! 演示:通过 Redis 桥向 B 侧服务发起一次 GET。
//!
//! 用法(url 是逻辑地址 https://{服务名}/path,服务名须在 agent 的 upstreams 里):
//!   REDIS_URL=redis://127.0.0.1:6379 BRIDGE_SERVICE=demo \
//!     cargo run -p bridge-client --example demo -- https://demo/api/foo

use std::time::{Duration, Instant};

use bridge_client::{BridgeClient, CallRequest, Config};
use bytes::Bytes;
use snafu::{prelude::*, Report, Whatever};

#[tokio::main]
async fn main() -> Report<Whatever> {
    run().await.into()
}

/// 真正的逻辑单独一层:返回 `Whatever` 让 `whatever_context` 的目标类型可推断,
/// main 再把它包成 `Report` 以获得展开 source 链的输出。
async fn run() -> Result<(), Whatever> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let service = std::env::var("BRIDGE_SERVICE").unwrap_or_else(|_| "demo".into());
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://demo/".into());

    let client = BridgeClient::start(Config::new(redis_url, service))
        .await
        .whatever_context("failed to start the bridge client")?;
    println!("instance_id = {}", client.instance_id());

    let started = Instant::now();
    let resp = client
        .call(CallRequest {
            method: "GET".into(),
            url,
            headers: vec![("accept".into(), "*/*".into())],
            body: Bytes::new(),
            timeout: Duration::from_secs(10),
        })
        .await
        .whatever_context("bridge call failed")?;

    println!("status  = {}", resp.status);
    println!("latency = {:?}", started.elapsed());
    for (name, value) in &resp.headers {
        println!("header  : {name}: {value}");
    }
    println!("body    = {}", String::from_utf8_lossy(&resp.body));
    Ok(())
}
