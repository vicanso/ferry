//! A 侧 client:对外唯一入口 `BridgeClient::call()`。
//!
//! 架构要点(design §3):回复队列按**实例**划分而非按请求划分。
//! 后台单任务 `reply_loop` 用一条专用连接 BRPOP 本实例的回复队列,
//! 并发路由在进程内用 `pending` map + oneshot 完成 —— 无论多少并发,
//! 只占用 1 条阻塞连接 + 1 个普通连接池。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use dashmap::DashMap;
use deadpool_redis::redis;
use deadpool_redis::{Pool, Runtime};
use snafu::prelude::*;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use bridge_protocol::{
    decode, encode, reply_queue, request_queue, BridgeError, CodecError, HttpOk, HttpRequest,
    HttpResponse, MAX_BODY_SIZE,
};

/// BRPOP 的阻塞时长。由于 BRPOP 发出后不可取消,这个值同时是
/// 「关闭 / 断线被感知」的延迟上限。
const BRPOP_TIMEOUT_SECS: u64 = 2;
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url: String,
    /// 目标服务名,对应请求队列 `bridge:req:{service}`。
    pub service: String,
}

impl Config {
    pub fn new(redis_url: impl Into<String>, service: impl Into<String>) -> Self {
        Self {
            redis_url: redis_url.into(),
            service: service.into(),
        }
    }
}

/// 一次调用的输入。`req_id` / `reply_to` / `deadline` 属于线上协议细节,
/// 由 client 内部填充,不对调用方暴露。
#[derive(Debug, Clone)]
pub struct CallRequest {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub timeout: Duration,
}

#[derive(Debug, Snafu)]
pub enum CallError {
    /// agent 显式回写的错误(design §2.2:可与「没收到」区分开)。
    #[snafu(display("agent reported a failure"))]
    Bridge { source: BridgeError },
    #[snafu(display("timed out after {timeout:?} waiting for a reply"))]
    Timeout { timeout: Duration },
    #[snafu(display("request body is {size} bytes, over the {limit} byte limit"))]
    TooLarge { size: usize, limit: usize },
    #[snafu(display("failed to encode the request"))]
    Encode { source: CodecError },
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
    #[snafu(display("client is shut down"))]
    Closed,
}

pub struct BridgeClient {
    pool: Pool,
    service: String,
    instance_id: String,
    pending: Arc<DashMap<Uuid, oneshot::Sender<HttpResponse>>>,
    shutdown: CancellationToken,
}

impl BridgeClient {
    /// 建池、生成 instance_id、spawn 后台 reply_loop。
    pub async fn start(cfg: Config) -> Result<Self, CallError> {
        let pool = deadpool_redis::Config::from_url(&cfg.redis_url)
            .create_pool(Some(Runtime::Tokio1))
            .context(CreatePoolSnafu)?;
        // fail fast:启动时先验证 Redis 可达
        {
            let mut conn = pool.get().await.context(PoolSnafu)?;
            redis::cmd("PING")
                .query_async::<()>(&mut conn)
                .await
                .context(RedisSnafu)?;
        }

        let instance_id = Uuid::new_v4().simple().to_string();
        let pending: Arc<DashMap<Uuid, oneshot::Sender<HttpResponse>>> =
            Arc::new(DashMap::new());
        let shutdown = CancellationToken::new();

        // reply_loop 独占一条连接做阻塞 BRPOP,不能从连接池借
        let dedicated = redis::Client::open(cfg.redis_url.as_str()).context(RedisConnectSnafu)?;
        tokio::spawn(reply_loop(
            dedicated,
            reply_queue(&instance_id),
            Arc::clone(&pending),
            shutdown.clone(),
        ));

        Ok(Self {
            pool,
            service: cfg.service,
            instance_id,
            pending,
            shutdown,
        })
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// 对外的唯一入口:发起一次 HTTP 调用并等待响应。
    pub async fn call(&self, req: CallRequest) -> Result<HttpOk, CallError> {
        ensure!(!self.shutdown.is_cancelled(), ClosedSnafu);
        ensure!(
            req.body.len() <= MAX_BODY_SIZE,
            TooLargeSnafu {
                size: req.body.len(),
                limit: MAX_BODY_SIZE,
            }
        );

        let timeout = req.timeout;
        let req_id = Uuid::new_v4();
        let wire = HttpRequest {
            req_id,
            reply_to: self.instance_id.clone(),
            method: req.method,
            uri: req.uri,
            headers: req.headers,
            body: req.body,
            deadline: unix_ms_now() + timeout.as_millis() as u64,
        };
        let payload = encode(&wire).context(EncodeSnafu)?;

        let (tx, rx) = oneshot::channel();
        self.pending.insert(req_id, tx);
        // RAII guard:超时/报错/正常返回都会移除 pending 条目,防止 map 泄漏(design §3.4)
        let _guard = PendingGuard {
            pending: &self.pending,
            req_id,
        };

        {
            let mut conn = self.pool.get().await.context(PoolSnafu)?;
            redis::cmd("LPUSH")
                .arg(request_queue(&self.service))
                .arg(&payload)
                .query_async::<i64>(&mut conn)
                .await
                .context(RedisSnafu)?;
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(resp)) => resp.result.context(BridgeSnafu),
            // sender 被丢弃且没发送 ⇒ reply_loop 已退出
            Ok(Err(_)) => ClosedSnafu.fail(),
            Err(_) => TimeoutSnafu { timeout }.fail(),
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl Drop for BridgeClient {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct PendingGuard<'a> {
    pending: &'a DashMap<Uuid, oneshot::Sender<HttpResponse>>,
    req_id: Uuid,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.pending.remove(&self.req_id);
    }
}

/// 后台单任务:一条专用连接,循环 BRPOP 本实例的回复队列并路由给等待者。
/// 断线按指数退避自愈(design §5.5)。
async fn reply_loop(
    client: redis::Client,
    queue: String,
    pending: Arc<DashMap<Uuid, oneshot::Sender<HttpResponse>>>,
    shutdown: CancellationToken,
) {
    let mut backoff = INITIAL_BACKOFF;
    let mut brpop = redis::cmd("BRPOP");
    brpop.arg(&queue).arg(BRPOP_TIMEOUT_SECS);
    'reconnect: loop {
        if shutdown.is_cancelled() {
            return;
        }
        let mut conn = tokio::select! {
            _ = shutdown.cancelled() => return,
            c = client.get_multiplexed_async_connection() => match c {
                Ok(c) => {
                    backoff = INITIAL_BACKOFF;
                    c
                }
                Err(e) => {
                    tracing::warn!(error = %e, ?backoff, "reply_loop: redis connect failed, retrying");
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue 'reconnect;
                }
            },
        };

        loop {
            if shutdown.is_cancelled() {
                return;
            }
            // 同 agent 的 pull_loop:BRPOP 发出后不可取消,否则已弹出的那条
            // 响应会随 future 一起丢失,对应的调用方只能干等到超时。
            let res: redis::RedisResult<Option<(String, Vec<u8>)>> =
                brpop.query_async(&mut conn).await;
            match res {
                Ok(Some((_key, raw))) => match decode::<HttpResponse>(&raw) {
                    Ok(resp) => {
                        if let Some((_, tx)) = pending.remove(&resp.req_id) {
                            // receiver 已 drop(调用方刚超时)也无妨,send 失败直接丢弃
                            let _ = tx.send(resp);
                        } else {
                            tracing::debug!(req_id = %resp.req_id, "late reply dropped");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "reply_loop: undecodable reply dropped");
                    }
                },
                // BRPOP 到时无消息,回头检查 shutdown 再继续
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "reply_loop: redis error, reconnecting");
                    continue 'reconnect;
                }
            }
        }
    }
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}
