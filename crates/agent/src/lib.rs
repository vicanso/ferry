//! B 侧 agent:BRPOP 拉取请求 → 转发本地 HTTP 服务 → 回写响应。
//!
//! 本 crate 既是库也是二进制。库只负责「跑 agent」这件事,**不装日志、不监听信号**
//! —— 那两样全局只能做一次,归入口管。这样同一个进程里可以再跑别的子系统
//! (见 `ferry` crate:agent 与 git-patch 服务同进程,共用一个 subscriber 和一个
//! [`CancellationToken`])。
//!
//! 三个要点(design §4.3):
//! 1. 先拿 semaphore 许可再 BRPOP —— 队列本身成为缓冲区(背压);
//! 2. 拉取到请求先查 deadline,已过期直接回 `Expired`,不打本地服务;
//! 3. 回写用 pipeline(LPUSH + EXPIRE),EXPIRE 是防泄漏保险丝。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

/// `agent.brpop_timeout_secs` 的默认值(与 `config/default.toml` 一致)。
const DEFAULT_BRPOP_TIMEOUT_SECS: u64 = 2;
/// `agent.metrics_interval_secs` 的默认值(与 `config/default.toml` 一致)。
/// `0` 表示关闭 metrics 循环。
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 30;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

fn default_brpop_timeout_secs() -> u64 {
    DEFAULT_BRPOP_TIMEOUT_SECS
}

fn default_metrics_interval_secs() -> u64 {
    DEFAULT_METRICS_INTERVAL_SECS
}

#[derive(Debug, Snafu)]
pub enum AgentError {
    #[snafu(display("agent.upstreams service {service:?} -> {url:?} is not a valid URL"))]
    InvalidUpstream {
        source: url::ParseError,
        service: String,
        url: String,
    },
    #[snafu(display(
        "agent.upstreams is empty; refusing to start (no service is reachable — \
         an empty map almost always means the upstream config is missing)"
    ))]
    EmptyUpstreams,
    #[snafu(display("agent.upstreams service {service:?} -> {url:?} must use http or https"))]
    NonHttpUpstream { service: String, url: String },
    #[snafu(display("agent.upstreams service {service:?} has an invalid header name {name:?}"))]
    InvalidHeaderName {
        source: reqwest::header::InvalidHeaderName,
        service: String,
        name: String,
    },
    // 只记 header 名,不记值 —— 注入的 header 往往是凭证,值绝不能进错误 / 日志。
    #[snafu(display("agent.upstreams service {service:?} header {name:?} has an invalid value"))]
    InvalidHeaderValue {
        source: reqwest::header::InvalidHeaderValue,
        service: String,
        name: String,
    },
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
    #[snafu(display(
        "agent.brpop_timeout_secs must be >= 1 (got {secs}); 0 would block forever and hang graceful shutdown"
    ))]
    ZeroBrpopTimeout { secs: u64 },
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
    /// 服务名 → 真实上游(base URL + 注入 header)。调用方只给服务名,真实地址与
    /// 注入的凭证都只存在这里、不进 Redis。
    upstreams: HashMap<String, Upstream>,
    sem: Arc<Semaphore>,
    service: String,
    /// Redis key 命名空间(前缀可配)。请求队列 / 回复都经它拼 key。
    keyspace: Keyspace,
    max_concurrency: usize,
    /// BRPOP 最长阻塞时长(默认 2s)。同时决定专用连接的 response timeout。
    brpop_timeout: Duration,
}

/// 一个上游服务的解析后配置。
struct Upstream {
    /// 真实 base URL(scheme+host+port,可带 base path)。
    base: Url,
    /// 转发时注入的 header,已在启动时解析校验。**覆盖调用方同名 header**(config wins),
    /// 因此适合放调用方不该经手的凭证(`Authorization` 等)。value 标记为 sensitive:
    /// 不进 `Debug` 输出、HTTP/2 下也不做 HPACK 索引。
    headers: Vec<(HeaderName, HeaderValue)>,
}

/// 烘焙进二进制的默认配置,保证不挂任何文件也能启动。
const DEFAULT_CONFIG: &str = include_str!("../config/default.toml");

/// 指向外部 TOML 的环境变量;不设则只用默认配置 + 环境变量覆盖。
const CONFIG_PATH_ENV: &str = "FERRY_CONFIG";
const ENV_PREFIX: &str = "FERRY";

/// `upstreams` 映射的两种写法:TOML 表(推荐),或单个 `"name=url,name2=url2"` 字符串。
///
/// 单字符串(CSV)是给环境变量的简写 —— 一个 env 变量带整张表,例如
/// `FERRY__AGENT__UPSTREAMS=grok=http://10.0.0.5:7257,api=http://127.0.0.1:8080`。
/// 因此 base URL 里不应含 `,` 或 `=`(纯 host[+basepath] 本就不该有)。CSV 形式
/// **只能表达 base、不能带注入 header**;要注入 header 用 TOML 表,或环境变量的嵌套写法
/// `FERRY__AGENT__UPSTREAMS__GROK__BASE=...` + `..__HEADERS__AUTHORIZATION=...`。
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum UpstreamSpec {
    Map(HashMap<String, UpstreamEntry>),
    Csv(String),
}

/// 单个 upstream 的写法:裸 URL 字符串(简写,无注入 header),或带 `base` + `headers`
/// 的表。untagged —— 字符串走 `Bare`、表走 `Full`,两者形状不同不会歧义。
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum UpstreamEntry {
    Bare(String),
    Full {
        base: String,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

impl Default for UpstreamSpec {
    fn default() -> Self {
        UpstreamSpec::Map(HashMap::new())
    }
}

impl UpstreamSpec {
    fn into_map(self) -> HashMap<String, UpstreamEntry> {
        match self {
            UpstreamSpec::Map(m) => m,
            UpstreamSpec::Csv(s) => s
                .split(',')
                .map(str::trim)
                .filter(|e| !e.is_empty())
                .filter_map(|e| e.split_once('='))
                .map(|(k, v)| (k.trim(), v.trim()))
                .filter(|(k, v)| !k.is_empty() && !v.is_empty())
                .map(|(k, v)| (k.to_owned(), UpstreamEntry::Bare(v.to_owned())))
                .collect(),
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
    /// 服务名 → 真实 base URL 的映射。缺省为空(将拒绝启动)。
    #[serde(default)]
    upstreams: UpstreamSpec,
    max_concurrency: usize,
    /// Redis key 前缀。缺省 / 留空回退到 `bridge`(见 Keyspace)。
    #[serde(default)]
    key_prefix: String,
    /// BRPOP 最长阻塞秒数。默认 [`DEFAULT_BRPOP_TIMEOUT_SECS`]。
    #[serde(default = "default_brpop_timeout_secs")]
    brpop_timeout_secs: u64,
    /// metrics `LLEN` 间隔秒数。默认 [`DEFAULT_METRICS_INTERVAL_SECS`];`0` 关闭。
    #[serde(default = "default_metrics_interval_secs")]
    metrics_interval_secs: u64,
}

pub struct AgentConfig {
    redis_url: String,
    service: String,
    upstreams: HashMap<String, Upstream>,
    max_concurrency: usize,
    key_prefix: String,
    brpop_timeout: Duration,
    /// `None` 表示不跑 metrics 循环。
    metrics_interval: Option<Duration>,
}

/// 按「烘焙默认值 → 外部文件 → 环境变量」三层叠加加载配置。
pub fn load_config() -> Result<AgentConfig, AgentError> {
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

    // upstreams 逐条校验:只接受 http(s) 的 base URL,header 名/值也在此解析,
    // typo / 非法值启动即失败;空映射拒绝启动(几乎必是配置漏了)。
    let mut upstreams = HashMap::new();
    for (service, entry) in agent.upstreams.into_map() {
        let (target, raw_headers) = match entry {
            UpstreamEntry::Bare(base) => (base, HashMap::new()),
            UpstreamEntry::Full { base, headers } => (base, headers),
        };
        let mut url = target.parse::<Url>().context(InvalidUpstreamSnafu {
            service: service.clone(),
            url: target.clone(),
        })?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            NonHttpUpstreamSnafu {
                service: service.clone(),
                url: target.clone(),
            }
        );
        // base 只应是 scheme+host+port[+basepath];query/fragment 无意义,清掉,
        // 免得后面按字符串拼 path 时污染结果。
        url.set_query(None);
        url.set_fragment(None);

        // 注入 header 在启动时就解析成 HeaderName/HeaderValue —— 非法配置立即报错,
        // 每请求只剩 clone。value 标记 sensitive(常是凭证):不进 Debug、h2 不索引。
        let mut headers = Vec::with_capacity(raw_headers.len());
        for (name, value) in raw_headers {
            let header_name =
                HeaderName::from_bytes(name.as_bytes()).context(InvalidHeaderNameSnafu {
                    service: service.clone(),
                    name: name.clone(),
                })?;
            let mut header_value =
                HeaderValue::from_str(&value).context(InvalidHeaderValueSnafu {
                    service: service.clone(),
                    name: name.clone(),
                })?;
            header_value.set_sensitive(true);
            headers.push((header_name, header_value));
        }

        upstreams.insert(service, Upstream { base: url, headers });
    }
    ensure!(!upstreams.is_empty(), EmptyUpstreamsSnafu);
    ensure!(agent.max_concurrency > 0, ZeroConcurrencySnafu);
    ensure!(
        agent.brpop_timeout_secs >= 1,
        ZeroBrpopTimeoutSnafu {
            secs: agent.brpop_timeout_secs
        }
    );

    let metrics_interval = if agent.metrics_interval_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(agent.metrics_interval_secs))
    };

    Ok(AgentConfig {
        redis_url: redis.uri,
        service: agent.service,
        upstreams,
        max_concurrency: agent.max_concurrency,
        key_prefix: agent.key_prefix,
        brpop_timeout: Duration::from_secs(agent.brpop_timeout_secs),
        metrics_interval,
    })
}

/// 把逻辑地址(`https://{服务名}/path?query`)解析成真实上游:定位服务配置并拼出目标 URL。
/// 返回 `(&Upstream, 目标 URL)` —— 调用方据此拿到该服务要注入的 header。
///
/// 服务名取自 URL 的 host 段,查 `upstreams` 得到真实 base URL(scheme+host+port,
/// 可带 base path);最终地址 = base + 请求 path + 请求 query,scheme/host/port 全部
/// 来自配置,调用方碰不到。服务名不在映射内返回 `UnknownUpstream`。
///
/// 安全性:调用方只能选服务名 + 路径,真实 host 完全由配置决定,天然无法指向任意
/// 内网地址 —— 这是比「白名单校验调用方给的真实 URL」更强的边界。若 base 带 path
/// (子树限制),额外拒绝含 `%2e`/`%2f` 的请求路径,防止上游解码后穿越出该子树。
fn resolve_upstream<'a>(
    upstreams: &'a HashMap<String, Upstream>,
    logical: &Url,
) -> Result<(&'a Upstream, Url), BridgeError> {
    let service = logical.host_str().ok_or_else(|| BridgeError::Internal {
        detail: format!("logical url has no service name: {logical}"),
    })?;
    let upstream = upstreams
        .get(service)
        .ok_or_else(|| BridgeError::UnknownUpstream {
            service: service.to_string(),
        })?;
    let base = &upstream.base;

    let base_path = base.path().trim_end_matches('/'); // "" 或 "/api"
    let req_path = logical.path(); // 解析后的路径,总以 "/" 开头
    if !base_path.is_empty() {
        let lower = req_path.to_ascii_lowercase();
        if lower.contains("%2e") || lower.contains("%2f") {
            return Err(BridgeError::Internal {
                detail: format!("encoded path traversal rejected: {req_path}"),
            });
        }
    }

    // 字符串拼接再 parse:避免 set_path 对已编码的 %XX 二次编码。base 已在
    // load_config 清掉 query/fragment,as_str() 就是纯 scheme://host[:port][/basepath]。
    let base_str = base.as_str().trim_end_matches('/');
    let mut full = format!("{base_str}{req_path}");
    if let Some(query) = logical.query() {
        full.push('?');
        full.push_str(query);
    }
    let target = full.parse::<Url>().map_err(|e| BridgeError::Internal {
        detail: format!("failed to build target url {full:?}: {e}"),
    })?;
    Ok((upstream, target))
}

/// 用给定配置启动 agent,直到 `shutdown` 被取消、in-flight 排空后返回。
///
/// 配置由调用方先用 [`load_config`] 取好再传进来:入口据此判断这个子系统要不要启用
/// (upstreams 为空时 `load_config` 报 [`AgentError::EmptyUpstreams`]),而不是让
/// 本函数自己去读配置然后无从区分「没配」和「配错了」。
pub async fn run(cfg: AgentConfig, shutdown: CancellationToken) -> Result<(), AgentError> {
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
        upstreams: cfg.upstreams,
        sem: Arc::new(Semaphore::new(cfg.max_concurrency)),
        service: cfg.service,
        keyspace,
        max_concurrency: cfg.max_concurrency,
        brpop_timeout: cfg.brpop_timeout,
    });

    if let Some(interval) = cfg.metrics_interval {
        tokio::spawn(metrics_loop(
            Arc::clone(&agent),
            interval,
            shutdown.clone(),
        ));
    } else {
        tracing::info!("metrics loop disabled (agent.metrics_interval_secs = 0)");
    }

    tracing::info!(
        service = %agent.service,
        redis_cluster = agent.redis.is_cluster(),
        // 只列服务名;真实上游地址是 B 侧内部信息,operator 需要时看配置即可
        upstreams = %agent.upstreams.keys().cloned().collect::<Vec<_>>().join(", "),
        max_concurrency = agent.max_concurrency,
        brpop_timeout_secs = agent.brpop_timeout.as_secs(),
        metrics_interval_secs = cfg.metrics_interval.map(|d| d.as_secs()).unwrap_or(0),
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
pub async fn shutdown_signal() {
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
pub async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// 主循环:先拿许可 → BRPOP 一条 → spawn 处理。断线指数退避自愈。
async fn pull_loop(agent: Arc<Agent>, shutdown: CancellationToken) -> Result<(), AgentError> {
    let queue = agent.keyspace.request_queue(&agent.service);
    let max_block = agent.brpop_timeout;
    // BRPOP 是阻塞命令,用专用连接,不从连接池借
    let mut conn: Option<bridge_redis::RedisDedicatedConn> = None;
    let mut backoff = INITIAL_BACKOFF;
    let mut brpop = redis::cmd("BRPOP");
    // 服务端超时由 tibba 的 helper 从 max_block 推导,和连接的 response timeout
    // 出自同一个来源,不会再出现两者对不上导致每次都超时的情况
    brpop.arg(&queue).arg(bridge_redis::brpop_timeout_secs(max_block));

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
                match agent.redis.dedicated_blocking_conn(max_block).await {
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
            // brpop_timeout 秒。
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
    // 从 Redis 读到的调用原样记一行(debug):method + 逻辑 url + 调用方 header。
    // 这些都是队列里已有的内容(redis-cli / ferry-dump 也看得到),记日志不构成
    // 额外泄露。注入的凭证 header 是配置里的、不在 Redis,绝不在这里打其值(见 forward)。
    tracing::debug!(
        %req_id,
        method = %req.method,
        url = %req.url,
        headers = ?req.headers,
        "request read from redis"
    );
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
/// 请求给的是逻辑地址(`https://{服务名}/...`),真实地址先由 `resolve_upstream`
/// 按配置解析出来;未知服务在发出任何网络请求之前就被拒绝。
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
    // 逻辑地址:host 是服务名,真实地址由 upstreams 配置解析。解析(含未知服务的
    // 拒绝)发生在任何网络请求之前 —— 调用方碰不到真实 host。
    let logical = req.url.parse::<Url>().map_err(|e| BridgeError::Internal {
        detail: format!("invalid url {:?}: {e}", req.url),
    })?;
    let (upstream, target) = match resolve_upstream(&agent.upstreams, &logical) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(url = %req.url, req_id = %req.req_id, error = %e, "rejected: upstream not resolved");
            return Err(e);
        }
    };

    // 只记注入了哪些 header 名,绝不记其值:值是配置里的凭证,刻意不进 Redis,
    // 也不该进日志 —— debug 排障时一旦被采集进中心化日志,泄露比进 Redis 更难收回。
    if !upstream.headers.is_empty() {
        tracing::debug!(
            req_id = %req.req_id,
            injected = ?upstream.headers.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            "injecting configured headers (values redacted)"
        );
    }
    // 调用方 header 先铺,再用该服务配置的注入 header 覆盖同名(config wins)。
    let headers = build_forward_headers(&req.headers, &upstream.headers);

    // 打点覆盖整个上游往返:发出 → 读完 body。用于下面的 elapsed_ms。
    let started = Instant::now();
    let resp = agent
        .http
        .request(method, target)
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

    // 成功路径上唯一的一行:status + body 字节数 + 整个往返耗时。
    // 这才是「completions 确实跑完了」的信号(前两条 debug 都在发送之前)。
    // 只记 status/尺寸/耗时,不打 resp body —— body 可能含用户内容且体积大,
    // 不该进日志;要看内容用调用方侧的响应或抓包。
    tracing::debug!(
        req_id = %req.req_id,
        status,
        bytes = body.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "upstream responded"
    );

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

/// 构造转发用的 `HeaderMap`:先铺调用方 header(剔 hop-by-hop 与 `accept-encoding` ——
/// 后者交给 reqwest 自行协商并透明解压,详见调用处),再用该服务配置的注入 header
/// **覆盖同名**(config wins,用 `insert` 替换掉调用方的全部同名值)。这样凭证之类可由
/// agent 配置强制注入,调用方既顶不掉也剥不掉。
fn build_forward_headers(
    caller: &[(String, String)],
    injected: &[(HeaderName, HeaderValue)],
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in caller {
        // accept-encoding 剥掉:若透传,reqwest 会认为调用方要自行处理压缩而**不再解压**,
        // body 就成了压缩流,协议层只能 base64 兜住,既不可读也违背「文本直存」的目的。
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
    // insert(非 append):替换调用方的全部同名值,保证注入值是该 header 的唯一值。
    for (name, value) in injected {
        headers.insert(name, value.clone());
    }
    headers
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
/// `interval` 来自 `agent.metrics_interval_secs`(默认 30s);为 0 时不 spawn 本任务。
async fn metrics_loop(agent: Arc<Agent>, interval: Duration, shutdown: CancellationToken) {
    let queue = agent.keyspace.request_queue(&agent.service);
    let mut tick = tokio::time::interval(interval);
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

    fn upstreams(pairs: &[(&str, &str)]) -> HashMap<String, Upstream> {
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    Upstream {
                        base: v.parse().unwrap(),
                        headers: Vec::new(),
                    },
                )
            })
            .collect()
    }

    fn resolve(pairs: &[(&str, &str)], logical: &str) -> Result<String, BridgeError> {
        resolve_upstream(&upstreams(pairs), &logical.parse().unwrap())
            .map(|(_, target)| target.to_string())
    }

    /// 已知服务名解析到配置的真实 base,path 原样带过去。
    #[test]
    fn known_service_resolves_to_real_base() {
        let up = [("grok", "http://10.0.0.5:7257")];
        assert_eq!(
            resolve(&up, "https://grok/v1/chat/completions").unwrap(),
            "http://10.0.0.5:7257/v1/chat/completions"
        );
    }

    /// query 原样保留。
    #[test]
    fn query_is_preserved() {
        let up = [("grok", "http://10.0.0.5:7257")];
        assert_eq!(
            resolve(&up, "https://grok/v1?a=1&b=2").unwrap(),
            "http://10.0.0.5:7257/v1?a=1&b=2"
        );
    }

    /// 调用方的 scheme / 端口只是占位,真实地址完全来自配置 —— 碰不到真实 host。
    #[test]
    fn caller_scheme_and_port_are_ignored() {
        let up = [("grok", "https://10.0.0.5:7257")];
        assert_eq!(
            resolve(&up, "http://grok:9999/x").unwrap(),
            "https://10.0.0.5:7257/x"
        );
    }

    /// base 带 path 时作为前缀,子树天然受限。
    #[test]
    fn base_path_is_prefixed() {
        let up = [("api", "http://127.0.0.1:8080/api")];
        assert_eq!(
            resolve(&up, "https://api/orders").unwrap(),
            "http://127.0.0.1:8080/api/orders"
        );
        assert_eq!(
            resolve(&up, "https://api/").unwrap(),
            "http://127.0.0.1:8080/api/"
        );
    }

    /// 服务名不在映射内 → UnknownUpstream,且不暴露任何真实地址。
    #[test]
    fn unknown_service_is_rejected() {
        let up = [("grok", "http://10.0.0.5:7257")];
        let err = resolve(&up, "https://nope/x").unwrap_err();
        assert!(matches!(err, BridgeError::UnknownUpstream { service } if service == "nope"));
    }

    /// base 带 path 时拒绝编码穿越;base 无 path(整个 host 可达)则不误伤编码字符。
    #[test]
    fn encoded_traversal_rejected_only_under_base_path() {
        let with_path = [("api", "http://127.0.0.1:8080/api")];
        assert!(resolve(&with_path, "https://api/x%2e%2e/y").is_err());
        assert!(resolve(&with_path, "https://api/a%2fb").is_err());

        let no_path = [("h", "http://127.0.0.1:8080")];
        assert!(resolve(&no_path, "https://h/items/a%20b").is_ok());
    }

    /// Url::parse 规范化 `..`,穿越也只会落在配置 host 内,越不出去。
    #[test]
    fn dot_segments_normalized_by_url_parse() {
        let up = [("grok", "http://10.0.0.5:7257")];
        assert_eq!(
            resolve(&up, "https://grok/../admin").unwrap(),
            "http://10.0.0.5:7257/admin"
        );
    }

    /// CSV 写法(环境变量简写)解析成一组 `Bare` 条目(不带注入 header)。
    #[test]
    fn upstream_spec_csv_parses() {
        let spec = UpstreamSpec::Csv(
            "grok=http://10.0.0.5:7257, api=http://127.0.0.1:8080/api".to_string(),
        );
        let map = spec.into_map();
        assert!(matches!(map.get("grok"), Some(UpstreamEntry::Bare(u)) if u == "http://10.0.0.5:7257"));
        assert!(matches!(map.get("api"), Some(UpstreamEntry::Bare(u)) if u == "http://127.0.0.1:8080/api"));
    }

    /// `Map` 里裸字符串走 `Bare`、带 base+headers 的表走 `Full`,into_map 原样透传。
    #[test]
    fn upstream_spec_map_keeps_full_entries() {
        let mut m = HashMap::new();
        m.insert(
            "grok".to_string(),
            UpstreamEntry::Bare("http://10.0.0.5:7257".to_string()),
        );
        m.insert(
            "api".to_string(),
            UpstreamEntry::Full {
                base: "http://127.0.0.1:8080/api".to_string(),
                headers: HashMap::from([("authorization".to_string(), "Bearer x".to_string())]),
            },
        );
        let out = UpstreamSpec::Map(m).into_map();
        assert!(matches!(out.get("grok"), Some(UpstreamEntry::Bare(_))));
        match out.get("api") {
            Some(UpstreamEntry::Full { base, headers }) => {
                assert_eq!(base, "http://127.0.0.1:8080/api");
                assert_eq!(headers.get("authorization").unwrap(), "Bearer x");
            }
            other => panic!("expected Full entry, got {other:?}"),
        }
    }

    /// 配置注入的 header 覆盖调用方同名(config wins);其余调用方 header 保留,
    /// accept-encoding 仍被剥掉。
    #[test]
    fn injected_headers_override_caller() {
        let injected = vec![(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer real"),
        )];
        let caller = vec![
            ("authorization".to_string(), "Bearer fake".to_string()),
            ("x-keep".to_string(), "1".to_string()),
            ("accept-encoding".to_string(), "gzip".to_string()),
        ];
        let headers = build_forward_headers(&caller, &injected);
        assert_eq!(
            headers.get("authorization").unwrap().to_str().unwrap(),
            "Bearer real"
        );
        assert_eq!(headers.get("x-keep").unwrap().to_str().unwrap(), "1");
        assert!(headers.get("accept-encoding").is_none());
    }

    /// 调用方没给的注入 header 直接加上。
    #[test]
    fn injected_headers_added_when_absent() {
        let injected = vec![(
            HeaderName::from_static("x-source"),
            HeaderValue::from_static("ferry"),
        )];
        let headers = build_forward_headers(&[], &injected);
        assert_eq!(headers.get("x-source").unwrap().to_str().unwrap(), "ferry");
    }
}
