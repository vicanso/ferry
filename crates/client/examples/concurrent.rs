//! 并发冒烟测试:单个 `BridgeClient` 同时发起 N 个请求。
//!
//! 验证 design §3 的核心主张 —— 无论并发多少,A 侧只占用 1 条阻塞连接
//! (reply_loop)+ 1 个普通连接池,响应靠 req_id 在进程内路由回各自的调用者。
//!
//! 用法(第二个参数是逻辑地址 https://{服务名}/path):
//!   BRIDGE_SERVICE=demo cargo run -p bridge-client --example concurrent -- 100 https://demo/index.html

use std::sync::Arc;
use std::time::{Duration, Instant};

use bridge_client::{BridgeClient, CallRequest, Config};
use bytes::Bytes;
use snafu::{prelude::*, Report, Whatever};

#[tokio::main]
async fn main() -> Report<Whatever> {
    run().await.into()
}

async fn run() -> Result<(), Whatever> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let service = std::env::var("BRIDGE_SERVICE").unwrap_or_else(|_| "demo".into());
    let mut args = std::env::args().skip(1);
    let n: usize = args
        .next()
        .unwrap_or_else(|| "100".into())
        .parse()
        .whatever_context("first argument must be a request count")?;
    let target = args
        .next()
        .unwrap_or_else(|| "https://demo/".into());

    let client = Arc::new(
        BridgeClient::start(Config::new(redis_url, service))
            .await
            .whatever_context("failed to start the bridge client")?,
    );

    let started = Instant::now();
    let mut tasks = Vec::with_capacity(n);
    for i in 0..n {
        let client = Arc::clone(&client);
        let url = target.clone();
        tasks.push(tokio::spawn(async move {
            let resp = client
                .call(CallRequest {
                    method: "GET".into(),
                    url,
                    headers: vec![("x-seq".into(), i.to_string())],
                    body: Bytes::new(),
                    timeout: Duration::from_secs(30),
                })
                .await;
            (i, resp)
        }));
    }

    let (mut ok, mut failed) = (0usize, 0usize);
    for task in tasks {
        match task.await.whatever_context("worker task panicked")? {
            (_, Ok(resp)) if resp.status == 200 => ok += 1,
            (i, Ok(resp)) => {
                failed += 1;
                eprintln!("#{i}: unexpected status {}", resp.status);
            }
            (i, Err(e)) => {
                failed += 1;
                eprintln!("#{i}: {}", Report::from_error(e));
            }
        }
    }

    let elapsed = started.elapsed();
    println!("concurrency = {n}");
    println!("ok          = {ok}");
    println!("failed      = {failed}");
    println!("wall clock  = {elapsed:?}");
    println!("throughput  = {:.0} req/s", n as f64 / elapsed.as_secs_f64());
    Ok(())
}
