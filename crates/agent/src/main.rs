//! B 侧 agent:BRPOP 拉取请求 → 转发本地 HTTP 服务 → 回写响应。
//!
//! 三个要点(design §4.3):
//! 1. 先拿 semaphore 许可再 BRPOP —— 队列本身成为缓冲区(背压);
//! 2. 拉取到请求先查 deadline,已过期直接回 `Expired`,不打本地服务;
//! 3. 回写用 pipeline(LPUSH + EXPIRE),EXPIRE 是防泄漏保险丝。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use deadpool_redis::redis;
use deadpool_redis::{Pool, Runtime};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use snafu::{prelude::*, Report};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use url::Url;

use bridge_protocol::{
    decode, encode, is_hop_by_hop, reply_queue, request_queue, BridgeError, HttpOk, HttpRequest,
    HttpResponse, MAX_BODY_SIZE, RESP_TTL_SECS,
};

/// BRPOP 的阻塞时长。由于 BRPOP 发出后不可取消,这个值同时是
/// 「关闭 / 断线被感知」的延迟上限。
const BRPOP_TIMEOUT_SECS: u64 = 2;
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const METRICS_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_MAX_CONCURRENCY: usize = 64;

#[derive(Debug, Snafu)]
enum AgentError {
    #[snafu(display("UPSTREAM_URL {url:?} is not a valid URL"))]
    UpstreamUrl { source: url::ParseError, url: String },
    #[snafu(display("MAX_CONCURRENCY {value:?} is not an integer"))]
    MaxConcurrency {
        source: std::num::ParseIntError,
        value: String,
    },
    #[snafu(display("MAX_CONCURRENCY must be greater than 0"))]
    ZeroConcurrency,
    #[snafu(display("failed to create the redis pool"))]
    CreatePool {
        source: deadpool_redis::CreatePoolError,
    },
    #[snafu(display("failed to open a dedicated redis connection"))]
    RedisConnect { source: redis::RedisError },
    #[snafu(display("redis pool is unavailable"))]
    Pool { source: deadpool_redis::PoolError },
    #[snafu(display("redis command failed"))]
    Redis { source: redis::RedisError },
    #[snafu(display("failed to encode the response"))]
    Encode { source: bridge_protocol::CodecError },
}

struct Agent {
    redis: Pool,
    http: reqwest::Client,
    upstream: Url,
    sem: Arc<Semaphore>,
    service: String,
    max_concurrency: usize,
}

struct AgentConfig {
    redis_url: String,
    service: String,
    upstream: Url,
    max_concurrency: usize,
}

fn config_from_env() -> Result<AgentConfig, AgentError> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let service = std::env::var("BRIDGE_SERVICE").unwrap_or_else(|_| "demo".into());
    let raw_upstream =
        std::env::var("UPSTREAM_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let upstream = raw_upstream.parse::<Url>().context(UpstreamUrlSnafu {
        url: raw_upstream.clone(),
    })?;
    let max_concurrency = match std::env::var("MAX_CONCURRENCY") {
        Ok(raw) => raw
            .parse::<usize>()
            .context(MaxConcurrencySnafu { value: raw.clone() })?,
        Err(_) => DEFAULT_MAX_CONCURRENCY,
    };
    ensure!(max_concurrency > 0, ZeroConcurrencySnafu);
    Ok(AgentConfig {
        redis_url,
        service,
        upstream,
        max_concurrency,
    })
}

#[tokio::main]
async fn main() -> Report<AgentError> {
    run().await.into()
}

async fn run() -> Result<(), AgentError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = config_from_env()?;
    let pool = deadpool_redis::Config::from_url(&cfg.redis_url)
        .create_pool(Some(Runtime::Tokio1))
        .context(CreatePoolSnafu)?;
    {
        // fail fast:启动时先验证 Redis 可达
        let mut conn = pool.get().await.context(PoolSnafu)?;
        redis::cmd("PING")
            .query_async::<()>(&mut conn)
            .await
            .context(RedisSnafu)?;
    }

    let agent = Arc::new(Agent {
        redis: pool,
        http: reqwest::Client::new(),
        upstream: cfg.upstream,
        sem: Arc::new(Semaphore::new(cfg.max_concurrency)),
        service: cfg.service,
        max_concurrency: cfg.max_concurrency,
    });

    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received; stop pulling, draining in-flight");
            shutdown.cancel();
        });
    }

    tokio::spawn(metrics_loop(Arc::clone(&agent), shutdown.clone()));

    tracing::info!(
        service = %agent.service,
        upstream = %agent.upstream,
        max_concurrency = agent.max_concurrency,
        "bridge agent started"
    );
    pull_loop(agent, cfg.redis_url, shutdown).await
}

/// 主循环:先拿许可 → BRPOP 一条 → spawn 处理。断线指数退避自愈。
async fn pull_loop(
    agent: Arc<Agent>,
    redis_url: String,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    let queue = request_queue(&agent.service);
    // BRPOP 是阻塞命令,用专用连接,不从连接池借
    let client = redis::Client::open(redis_url.as_str()).context(RedisConnectSnafu)?;
    let mut conn: Option<redis::aio::MultiplexedConnection> = None;
    let mut backoff = INITIAL_BACKOFF;
    let mut brpop = redis::cmd("BRPOP");
    brpop.arg(&queue).arg(BRPOP_TIMEOUT_SECS);

    'main: loop {
        // ① 先拿许可再拉取,顺序不能反:让请求积压在 Redis 队列里而不是本进程内存里
        let permit = tokio::select! {
            _ = shutdown.cancelled() => break 'main,
            p = Arc::clone(&agent.sem).acquire_owned() => p.expect("semaphore never closed"),
        };

        // ② 拉一条消息
        let raw = 'pull: loop {
            // shutdown 只在两次 BRPOP 之间检查,见下方 await 处的说明
            if shutdown.is_cancelled() {
                break 'pull None;
            }
            if conn.is_none() {
                match client.get_multiplexed_async_connection().await {
                    Ok(c) => {
                        backoff = INITIAL_BACKOFF;
                        conn = Some(c);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, ?backoff, "pull_loop: redis connect failed, retrying");
                        tokio::select! {
                            _ = shutdown.cancelled() => break 'pull None,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue 'pull;
                    }
                }
            }
            let c = conn.as_mut().expect("connection just established");
            // 绝不能把这个 await 放进 tokio::select! 里取消:命令一旦发给 Redis,
            // 元素就已从 list 弹出,丢弃 future 等于把这条请求扔掉 —— 它既不在
            // 队列里也没人处理,调用方只能干等到超时。宁可让关闭最多多等
            // BRPOP_TIMEOUT_SECS 秒。
            let res: redis::RedisResult<Option<(String, Vec<u8>)>> = brpop.query_async(c).await;
            match res {
                Ok(Some((_key, raw))) => break 'pull Some(raw),
                // BRPOP 到时无消息,回头检查 shutdown 再继续
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "pull_loop: redis error, reconnecting");
                    conn = None;
                }
            }
        };

        match raw {
            Some(raw) => {
                tokio::spawn(handle_one(Arc::clone(&agent), raw, permit));
            }
            None => {
                drop(permit);
                break 'main;
            }
        }
    }

    // 优雅关闭:许可全部归还即 in-flight 排空(design §5.1)
    tracing::info!("draining in-flight requests");
    let _ = agent.sem.acquire_many(agent.max_concurrency as u32).await;
    tracing::info!("drained; exiting");
    Ok(())
}

/// 处理单条请求。无论成败都回写一条 `HttpResponse`(design §2.2)。
async fn handle_one(agent: Arc<Agent>, raw: Vec<u8>, _permit: OwnedSemaphorePermit) {
    let req: HttpRequest = match decode(&raw) {
        Ok(r) => r,
        Err(e) => {
            // 解码失败连 reply_to 都拿不到,只能丢弃并记日志。这条日志是排查
            // 跨语言协议不一致的唯一线索,必须带上 serde 的具体报错。
            tracing::warn!(
                error = %Report::from_error(e),
                payload = %String::from_utf8_lossy(&raw),
                "dropping undecodable request"
            );
            return;
        }
    };

    let req_id = req.req_id;
    let result = forward(&agent, &req).await;
    if let Err(ref e) = result {
        tracing::debug!(%req_id, error = %e, "request failed");
    }
    if let Err(e) = send_response(&agent, &req.reply_to, &HttpResponse { req_id, result }).await {
        // Report 展开整条 source 链,否则运维只能看到最外层那句话
        tracing::warn!(%req_id, error = %Report::from_error(e), "failed to write response back to redis");
    }
}

/// 转发到本地 upstream,返回 Result 语义的结果。
async fn forward(agent: &Agent, req: &HttpRequest) -> Result<HttpOk, BridgeError> {
    // deadline 先检查:A 已超时离开就不打本地服务,避免无意义负载与重复副作用
    let now = unix_ms_now();
    if now >= req.deadline {
        return Err(BridgeError::Expired);
    }
    let remaining = Duration::from_millis(req.deadline - now);

    if req.body.len() > MAX_BODY_SIZE {
        return Err(BridgeError::TooLarge {
            limit: MAX_BODY_SIZE,
        });
    }

    // BridgeError 要跨网络传输,变体只能带纯数据,故把源错误折叠进 detail
    let method = reqwest::Method::from_bytes(req.method.as_bytes()).map_err(|e| {
        BridgeError::Internal {
            detail: format!("invalid method {:?}: {e}", req.method),
        }
    })?;
    let url = agent.upstream.join(&req.uri).map_err(|e| BridgeError::Internal {
        detail: format!("invalid uri {:?}: {e}", req.uri),
    })?;

    let mut headers = HeaderMap::new();
    for (name, value) in &req.headers {
        if is_hop_by_hop(name) {
            continue;
        }
        let (Ok(n), Ok(v)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            tracing::debug!(header = %name, "dropping invalid header");
            continue;
        };
        headers.append(n, v);
    }

    let resp = agent
        .http
        .request(method, url)
        .headers(headers)
        .body(req.body.clone())
        .timeout(remaining)
        .send()
        .await
        .map_err(map_reqwest_err)?;

    let status = resp.status().as_u16();
    // header value 理论上可含非 ASCII 字节,实践中不会;lossy 转换换取 JSON 可读
    let resp_headers = resp
        .headers()
        .iter()
        .filter(|(name, _)| !is_hop_by_hop(name.as_str()))
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let body = resp.bytes().await.map_err(map_reqwest_err)?;
    if body.len() > MAX_BODY_SIZE {
        return Err(BridgeError::TooLarge {
            limit: MAX_BODY_SIZE,
        });
    }

    Ok(HttpOk {
        status,
        headers: resp_headers,
        body,
    })
}

fn map_reqwest_err(e: reqwest::Error) -> BridgeError {
    if e.is_timeout() {
        BridgeError::UpstreamTimeout
    } else if e.is_connect() {
        BridgeError::UpstreamUnreachable {
            detail: e.to_string(),
        }
    } else {
        BridgeError::Internal {
            detail: e.to_string(),
        }
    }
}

/// 回写响应:LPUSH + EXPIRE 合并为一次往返;EXPIRE 保证 A 实例
/// 崩溃/下线后其回复队列不会永远留在 Redis 里。
async fn send_response(
    agent: &Agent,
    reply_to: &str,
    resp: &HttpResponse,
) -> Result<(), AgentError> {
    let payload = encode(resp).context(EncodeSnafu)?;
    let key = reply_queue(reply_to);
    let mut conn = agent.redis.get().await.context(PoolSnafu)?;
    redis::pipe()
        .cmd("LPUSH")
        .arg(&key)
        .arg(payload)
        .ignore()
        .cmd("EXPIRE")
        .arg(&key)
        .arg(RESP_TTL_SECS)
        .ignore()
        .query_async::<()>(&mut conn)
        .await
        .context(RedisSnafu)?;
    Ok(())
}

/// 可观测性(design §5.4):周期性输出队列积压(最重要的健康指标)与 in-flight 并发。
async fn metrics_loop(agent: Arc<Agent>, shutdown: CancellationToken) {
    let queue = request_queue(&agent.service);
    let mut tick = tokio::time::interval(METRICS_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        match agent.redis.get().await {
            Ok(mut conn) => {
                match redis::cmd("LLEN")
                    .arg(&queue)
                    .query_async::<i64>(&mut conn)
                    .await
                {
                    Ok(backlog) => tracing::info!(
                        backlog,
                        in_flight = agent.max_concurrency - agent.sem.available_permits(),
                        "bridge metrics"
                    ),
                    Err(e) => tracing::warn!(error = %e, "metrics: LLEN failed"),
                }
            }
            Err(e) => tracing::warn!(error = %e, "metrics: redis pool unavailable"),
        }
    }
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}
