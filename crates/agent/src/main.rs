//! B 侧 agent:BRPOP 拉取请求 → 转发本地 HTTP 服务 → 回写响应。
//!
//! 三个要点(design §4.3):
//! 1. 先拿 semaphore 许可再 BRPOP —— 队列本身成为缓冲区(背压);
//! 2. 拉取到请求先查 deadline,已过期直接回 `Expired`,不打本地服务;
//! 3. 回写用 pipeline(LPUSH + EXPIRE),EXPIRE 是防泄漏保险丝。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bridge_redis::RedisClient;
use bridge_redis::redis;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use snafu::{prelude::*, Report};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use url::Url;

use bridge_protocol::{
    decode, encode, is_hop_by_hop, BridgeError, HttpOk, HttpRequest, HttpResponse, Keyspace,
    ReplyMode, MAX_BODY_SIZE, RESP_KV_TTL_SECS, RESP_TTL_SECS,
};

/// BRPOP 的最长阻塞时长。由于 BRPOP 发出后不可取消,这个值同时是
/// 「关闭 / 断线被感知」的延迟上限。
/// 它同时决定专用连接的 response timeout —— 由 tibba 依此推导,见
/// `dedicated_blocking_conn`。
const MAX_BLOCK: Duration = Duration::from_secs(2);
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const METRICS_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Snafu)]
enum AgentError {
    #[snafu(display("agent.allowed_upstreams entry {url:?} is not a valid URL"))]
    AllowedUpstream { source: url::ParseError, url: String },
    #[snafu(display(
        "agent.allowed_upstreams is empty; refusing to start (an agent with no allow list \
         would be an open proxy into this network)"
    ))]
    EmptyAllowList,
    #[snafu(display("agent.allowed_upstreams entry {url:?} must use http or https"))]
    NonHttpUpstream { url: String },
    #[snafu(display("failed to read the config file {path:?}"))]
    ReadConfig {
        source: std::io::Error,
        path: String,
    },
    #[snafu(display("failed to load the configuration"))]
    Config { source: tibba_config::Error },
    #[snafu(display("failed to build the http client: {detail}"))]
    HttpClient { detail: String },
    #[snafu(display("agent.max_concurrency must be greater than 0"))]
    ZeroConcurrency,
    #[snafu(display("redis connection failed"))]
    Redis { source: bridge_redis::Error },
    /// 从 tibba-cache 借用连接失败(池耗尽、连接建立失败等)
    #[snafu(display("failed to acquire a redis connection"))]
    Acquire { source: bridge_redis::CacheError },
    #[snafu(display("redis command failed"))]
    Command { source: redis::RedisError },
    #[snafu(display("failed to encode the response"))]
    Encode { source: bridge_protocol::CodecError },
}

struct Agent {
    redis: Arc<RedisClient>,
    http: reqwest::Client,
    /// 允许被访问的上游清单。目标 URL 由请求方给出,但必须落在这里面。
    allowed: Vec<Url>,
    sem: Arc<Semaphore>,
    service: String,
    /// Redis key 命名空间(前缀可配)。请求队列 / 回复都经它拼 key。
    keyspace: Keyspace,
    max_concurrency: usize,
}

/// 烘焙进二进制的默认配置,保证不挂任何文件也能启动。
const DEFAULT_CONFIG: &str = include_str!("../config/default.toml");

/// 指向外部 TOML 的环境变量;不设则只用默认配置 + 环境变量覆盖。
const CONFIG_PATH_ENV: &str = "FERRY_CONFIG";
const ENV_PREFIX: &str = "FERRY";

/// 既接受 TOML 数组,也接受逗号分隔的字符串。
///
/// 环境变量只能表达后者 —— tibba-config 没给 config-rs 设 `list_separator`,
/// 所以 `FERRY__AGENT__ALLOWED_UPSTREAMS=a,b` 反序列化成 `Vec<String>` 会失败。
/// 用 untagged 同时容纳两种写法,配置文件里可以写得好看,环境变量也能覆盖。
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringList {
    Csv(String),
    List(Vec<String>),
}

impl StringList {
    fn into_vec(self) -> Vec<String> {
        match self {
            StringList::Csv(s) => s
                .split(',')
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
                .collect(),
            StringList::List(v) => v.into_iter().filter(|s| !s.trim().is_empty()).collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawRedis {
    uri: String,
}

#[derive(Debug, Deserialize)]
struct RawAgent {
    service: String,
    allowed_upstreams: StringList,
    max_concurrency: usize,
    /// Redis key 前缀。缺省 / 留空回退到 `bridge`(见 Keyspace)。
    #[serde(default)]
    key_prefix: String,
}

struct AgentConfig {
    redis_url: String,
    service: String,
    allowed: Vec<Url>,
    max_concurrency: usize,
    key_prefix: String,
}

/// 按「烘焙默认值 → 外部文件 → 环境变量」三层叠加加载配置。
fn load_config() -> Result<AgentConfig, AgentError> {
    let mut builder = tibba_config::Config::builder().add_toml(DEFAULT_CONFIG);
    if let Ok(path) = std::env::var(CONFIG_PATH_ENV) {
        let data = std::fs::read_to_string(&path).context(ReadConfigSnafu { path })?;
        builder = builder.add_toml(data);
    }
    let config = builder
        .with_env_prefix(ENV_PREFIX)
        .build()
        .context(ConfigSnafu)?;

    // 按段取,不在根配置上整体反序列化:tibba-config 的 `try_deserialize` 在
    // prefix 为空时会拿空字符串当 key 去查,config-rs 解析空 key 直接报
    // "invalid identifier"。分段读时 prefix 非空,绕开该问题。
    let redis: RawRedis = config
        .sub_config("redis")
        .try_deserialize()
        .context(ConfigSnafu)?;
    let agent: RawAgent = config
        .sub_config("agent")
        .try_deserialize()
        .context(ConfigSnafu)?;

    // 允许清单必须逐条校验:只接受 http(s),且不能为空 —— 空清单等于开放代理
    let mut allowed = Vec::new();
    for entry in agent.allowed_upstreams.into_vec() {
        let url = entry
            .parse::<Url>()
            .context(AllowedUpstreamSnafu { url: entry.clone() })?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            NonHttpUpstreamSnafu { url: entry }
        );
        allowed.push(url);
    }
    ensure!(!allowed.is_empty(), EmptyAllowListSnafu);
    ensure!(agent.max_concurrency > 0, ZeroConcurrencySnafu);

    Ok(AgentConfig {
        redis_url: redis.uri,
        service: agent.service,
        allowed,
        max_concurrency: agent.max_concurrency,
        key_prefix: agent.key_prefix,
    })
}

/// 目标 URL 是否落在允许清单内。
///
/// 匹配规则:
/// 1. **origin(scheme + host + port)必须完全相同** —— 这是硬边界。任何
///    主机名、端口或协议的差异都会被拒绝,`file:`/`data:` 这类不透明 origin
///    也因此天然不匹配。
/// 2. 清单项若带路径,则目标路径必须落在它的子树内,且按**路径段边界**比较,
///    以免 `/api` 意外放行 `/apifoo`。清单项路径为空或 `/` 时不做路径限制。
///
/// `Url::parse` 会规范化 `.` 与 `..`,所以普通的路径穿越进不来;但百分号编码的
/// `%2e` / `%2f` 不会被解码,上游服务器却可能解码它们,因此在有路径限制时直接
/// 拒绝含这两者的路径。origin 才是可靠的边界,路径限制属于纵深防御。
fn is_allowed(target: &Url, allowed: &[Url]) -> bool {
    allowed.iter().any(|entry| {
        if entry.origin() != target.origin() {
            return false;
        }
        let base = entry.path().trim_end_matches('/');
        if base.is_empty() {
            return true;
        }
        let path = target.path();
        if path.to_ascii_lowercase().contains("%2e") || path.to_ascii_lowercase().contains("%2f") {
            return false;
        }
        path == base || path.starts_with(&format!("{base}/"))
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

    let cfg = load_config()?;
    // 单节点还是集群由 bridge-redis 判定(节点数 + CLUSTER INFO 探测),
    // 这里不关心拓扑。connect 内部已做过 PING 的可达性验证。
    let redis = Arc::new(bridge_redis::connect(&cfg.redis_url).context(RedisSnafu)?);

    // 必须禁用自动跟随重定向:否则上游只要回一个 302 指向内网地址,agent 就会
    // 跟过去,允许清单形同虚设。3xx 原样回给调用方,由它自己决定要不要跟。
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| AgentError::HttpClient { detail: e.to_string() })?;

    let keyspace = Keyspace::new(&cfg.key_prefix);
    tracing::info!(key_prefix = keyspace.prefix(), "redis keyspace");

    let agent = Arc::new(Agent {
        redis,
        http,
        allowed: cfg.allowed,
        sem: Arc::new(Semaphore::new(cfg.max_concurrency)),
        service: cfg.service,
        keyspace,
        max_concurrency: cfg.max_concurrency,
    });

    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            tracing::info!("shutdown signal received; stop pulling, draining in-flight");
            shutdown.cancel();
        });
    }

    tokio::spawn(metrics_loop(Arc::clone(&agent), shutdown.clone()));

    tracing::info!(
        service = %agent.service,
        redis_cluster = agent.redis.is_cluster(),
        allowed_upstreams = %agent.allowed.iter().map(Url::as_str).collect::<Vec<_>>().join(", "),
        max_concurrency = agent.max_concurrency,
        "bridge agent started"
    );
    pull_loop(agent, shutdown).await
}

/// 等待终止信号。
///
/// 必须同时监听 SIGTERM:容器编排器(`docker stop`、k8s)发的是 SIGTERM,
/// 只监听 SIGINT 的话进程收不到任何信号,会在 grace period 结束后被 SIGKILL
/// 强杀,in-flight 请求全部丢失 —— 优雅关闭形同虚设。
#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "cannot install SIGTERM handler; only SIGINT will work");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!(signal = "SIGINT", "signal received"),
        _ = sigterm.recv() => tracing::info!(signal = "SIGTERM", "signal received"),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// 主循环:先拿许可 → BRPOP 一条 → spawn 处理。断线指数退避自愈。
async fn pull_loop(agent: Arc<Agent>, shutdown: CancellationToken) -> Result<(), AgentError> {
    let queue = agent.keyspace.request_queue(&agent.service);
    // BRPOP 是阻塞命令,用专用连接,不从连接池借
    let mut conn: Option<bridge_redis::RedisDedicatedConn> = None;
    let mut backoff = INITIAL_BACKOFF;
    let mut brpop = redis::cmd("BRPOP");
    // 服务端超时由 tibba 的 helper 从 MAX_BLOCK 推导,和连接的 response timeout
    // 出自同一个来源,不会再出现两者对不上导致每次都超时的情况
    brpop.arg(&queue).arg(bridge_redis::brpop_timeout_secs(MAX_BLOCK));

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
                match agent.redis.dedicated_blocking_conn(MAX_BLOCK).await {
                    // 这里**不能**重置 backoff。像 MOVED 这种「连得上但每条命令都
                    // 失败」的故障,重连总是成功的,在这里重置会让退避永远归零、
                    // 退化成热循环。只有命令真正成功才算恢复。
                    Ok(c) => conn = Some(c),
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
                Ok(Some((_key, raw))) => {
                    backoff = INITIAL_BACKOFF;
                    break 'pull Some(raw);
                }
                // BRPOP 到时无消息 —— 这也说明链路是通的,可以重置退避
                Ok(None) => backoff = INITIAL_BACKOFF,
                Err(e) => {
                    tracing::warn!(error = %e, ?backoff, "pull_loop: redis command failed, retrying");
                    conn = None;
                    // 命令级错误同样要退避。少了这一步,持续性故障(例如集群拓扑
                    // 判错导致的 MOVED)会变成每秒几十次的热循环,既刷爆日志也
                    // 白白压 Redis。
                    tokio::select! {
                        _ = shutdown.cancelled() => break 'pull None,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
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
    if let Err(e) = send_response(&agent, &req, &HttpResponse { req_id, result }).await {
        // Report 展开整条 source 链,否则运维只能看到最外层那句话
        tracing::warn!(%req_id, error = %Report::from_error(e), "failed to write response back to redis");
    }
}

/// 转发到目标上游,返回 Result 语义的结果。
///
/// 目标地址来自请求,但必须先通过 `is_allowed` 的清单校验。
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
    // 必须解析为绝对 URL:Url::parse 不接受相对引用,因此不存在「拼到 base 上」
    // 的余地,目标地址完全显式。
    let url = req.url.parse::<Url>().map_err(|e| BridgeError::Internal {
        detail: format!("invalid url {:?}: {e}", req.url),
    })?;
    // 安全边界:清单之外一律拒绝,且在发出任何网络请求之前就拒绝
    if !is_allowed(&url, &agent.allowed) {
        tracing::warn!(%url, req_id = %req.req_id, "rejected: upstream not in allow list");
        return Err(BridgeError::UpstreamNotAllowed {
            url: url.to_string(),
        });
    }

    let mut headers = HeaderMap::new();
    for (name, value) in &req.headers {
        // hop-by-hop 不透传;accept-encoding 也剥掉 —— 交给 reqwest 自己协商并透明
        // 解压。若把调用方带来的 accept-encoding 透传过去,reqwest 会认为调用方要
        // 自行处理压缩而**不再解压**,body 就成了压缩流,协议层只能 base64 兜住,
        // 既不可读也违背这次「文本直存」的目的。
        if is_hop_by_hop(name) || name.eq_ignore_ascii_case("accept-encoding") {
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

/// 回写响应。两种投递方式都带 TTL,保证 A 侧崩溃/不来取时 Redis 不会永久残留:
/// - `Queue`:`LPUSH bridge:resp:{reply_to}` + `EXPIRE`,调用方 BRPOP 阻塞取。
/// - `Kv`:`SET bridge:resp:{req_id} <resp> EX`,调用方之后按 req_id GET 自取。
async fn send_response(
    agent: &Agent,
    req: &HttpRequest,
    resp: &HttpResponse,
) -> Result<(), AgentError> {
    let payload = encode(resp).context(EncodeSnafu)?;
    let mut conn = agent.redis.conn().await.context(AcquireSnafu)?;
    match req.reply_mode {
        ReplyMode::Queue => {
            let key = agent.keyspace.reply_queue(&req.reply_to);
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
                .context(CommandSnafu)?;
        }
        ReplyMode::Kv => {
            // SET 自带 EX,一次往返即写入 + 设 TTL,无需 pipeline。
            let key = agent.keyspace.response_kv_key(&req.req_id);
            redis::cmd("SET")
                .arg(&key)
                .arg(payload)
                .arg("EX")
                .arg(RESP_KV_TTL_SECS)
                .query_async::<()>(&mut conn)
                .await
                .context(CommandSnafu)?;
        }
    }
    Ok(())
}

/// 可观测性(design §5.4):周期性输出队列积压(最重要的健康指标)与 in-flight 并发。
async fn metrics_loop(agent: Arc<Agent>, shutdown: CancellationToken) {
    let queue = agent.keyspace.request_queue(&agent.service);
    let mut tick = tokio::time::interval(METRICS_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        match agent.redis.conn().await {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(entries: &[&str]) -> Vec<Url> {
        entries.iter().map(|e| e.parse().unwrap()).collect()
    }

    fn allowed(list: &[&str], target: &str) -> bool {
        is_allowed(&target.parse().unwrap(), &allow(list))
    }

    #[test]
    fn same_origin_passes() {
        let list = ["http://127.0.0.1:8080"];
        assert!(allowed(&list, "http://127.0.0.1:8080/"));
        assert!(allowed(&list, "http://127.0.0.1:8080/api/orders?page=1"));
    }

    /// origin 的任一部分不同都必须拒绝,这是硬边界。
    #[test]
    fn different_origin_rejected() {
        let list = ["http://127.0.0.1:8080"];
        assert!(!allowed(&list, "http://127.0.0.1:8081/"), "端口不同");
        assert!(!allowed(&list, "http://127.0.0.2:8080/"), "主机不同");
        assert!(!allowed(&list, "https://127.0.0.1:8080/"), "协议不同");
        assert!(!allowed(&list, "http://evil.example/"), "完全无关的主机");
        assert!(
            !allowed(&list, "http://169.254.169.254/latest/meta-data/"),
            "云元数据服务必须拒绝"
        );
    }

    /// 非 http(s) 的 scheme 产生不透明 origin,永远匹配不上。
    #[test]
    fn non_http_schemes_rejected() {
        let list = ["http://127.0.0.1:8080"];
        assert!(!allowed(&list, "file:///etc/passwd"));
        assert!(!allowed(&list, "data:text/plain,hi"));
    }

    #[test]
    fn multiple_entries() {
        let list = ["http://127.0.0.1:8080", "http://127.0.0.1:9090"];
        assert!(allowed(&list, "http://127.0.0.1:8080/a"));
        assert!(allowed(&list, "http://127.0.0.1:9090/b"));
        assert!(!allowed(&list, "http://127.0.0.1:7070/c"));
    }

    /// 清单项带路径时限制在子树内,且必须按段边界比较。
    #[test]
    fn path_scoping() {
        let list = ["http://127.0.0.1:8080/api"];
        assert!(allowed(&list, "http://127.0.0.1:8080/api"));
        assert!(allowed(&list, "http://127.0.0.1:8080/api/orders"));
        assert!(!allowed(&list, "http://127.0.0.1:8080/admin"));
        assert!(
            !allowed(&list, "http://127.0.0.1:8080/apifoo"),
            "/api 不能放行 /apifoo"
        );
    }

    /// Url::parse 会规范化 `..`,所以穿越后落在清单外就会被拒。
    #[test]
    fn dot_segments_are_normalized_then_checked() {
        let list = ["http://127.0.0.1:8080/api"];
        let u: Url = "http://127.0.0.1:8080/api/../admin".parse().unwrap();
        assert_eq!(u.path(), "/admin", "解析阶段已规范化");
        assert!(!is_allowed(&u, &allow(&list)));
    }

    /// 百分号编码的 `.` `/` 不会被 Url 解码,但上游可能解码,因此有路径限制时直接拒绝。
    #[test]
    fn percent_encoded_traversal_rejected() {
        let list = ["http://127.0.0.1:8080/api"];
        assert!(!allowed(&list, "http://127.0.0.1:8080/api/%2e%2e/admin"));
        assert!(!allowed(&list, "http://127.0.0.1:8080/api/%2E%2E/admin"));
        assert!(!allowed(&list, "http://127.0.0.1:8080/api/x%2fy"));
    }

    /// 清单项只有 origin(路径为空或 /)时不做路径限制,编码字符也不该误伤。
    #[test]
    fn origin_only_entry_allows_any_path() {
        let list = ["http://127.0.0.1:8080/"];
        assert!(allowed(&list, "http://127.0.0.1:8080/anything/at/all"));
        assert!(allowed(&list, "http://127.0.0.1:8080/items/a%20b"));
    }
}
