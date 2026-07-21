# Redis HTTP Bridge — 架构设计

> **背景**:两台机器 A、B 之间网络不通,但都能连接同一个 Redis 实例。
> **目标**:让 A 能够调用 B 上的 HTTP 服务。
> **手段**:把 Redis 当作反向隧道的传输层,在其上实现一套一问一答的 RPC。

---

## 0. 整体思路

核心是**反转连接方向**:B 不再被动等待 A 连过来,而是主动去 Redis 拉取任务。这样两端都是**出向连接**,绕开了 A 无法直连 B 的限制。

```
A(调用方)                  Redis                    B(有 HTTP 服务)
   |                         |                          |
   | 1. 生成 req_id           |                          |
   | 2. LPUSH 请求 ---------> │  bridge:req:{service}    |
   |                         │ <----- 3. BRPOP 阻塞拉取   |
   |                         │                          | 4. 在本机发真正的 HTTP
   | 5. 等待回复(见 §3)      │                          |    请求到 127.0.0.1:8080
   |                         │ <----- 6. LPUSH 响应       |
   | 7. 收到响应 <----------- │  bridge:resp:{instance}  |
```

关键机制:**correlation id(`req_id`)关联请求与响应**。

---

## 1. Workspace 划分

```
redis-http-bridge/
├── Cargo.toml            # workspace
└── crates/
    ├── protocol/         # 共享:消息定义 + 编解码 + key 约定
    ├── client/           # A 侧:发起方,对外暴露 call()
    └── agent/            # B 侧:拉取 → 转发本地 HTTP → 回写
```

**为什么 `protocol` 必须独立成 crate**:A 和 B 是两个独立部署的二进制,协议一旦不同步就是隐性事故(解码失败、字段错位)。共享 crate 让一致性在编译期就被锁死。

---

## 2. protocol — 协议层

### 2.1 消息定义

```rust
use bytes::Bytes;
use uuid::Uuid;
use serde::{Serialize, Deserialize};

/// 请求信封
#[derive(Serialize, Deserialize)]
pub struct HttpRequest {
    pub req_id: Uuid,
    pub reply_to: String,          // A 实例的回复队列名(见 §3)
    pub method: String,            // GET / POST / ...
    pub uri: String,               // /api/foo?x=1
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Bytes,               // 原始二进制,不做 base64
    pub deadline: u64,             // Unix 毫秒,绝对时间
}

/// 响应信封 —— 注意是 Result 语义
#[derive(Serialize, Deserialize)]
pub struct HttpResponse {
    pub req_id: Uuid,
    pub result: Result<HttpOk, BridgeError>,
}

#[derive(Serialize, Deserialize)]
pub struct HttpOk {
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
    pub body: Bytes,
}

#[derive(Serialize, Deserialize)]
pub enum BridgeError {
    UpstreamUnreachable(String),   // B 连不上本地服务
    UpstreamTimeout,               // 本地服务超时
    Expired,                       // 拉取到时已超过 deadline
    TooLarge { limit: usize },     // 超出 body 大小限制
    Internal(String),
}
```

### 2.2 关键设计决策

**响应里带 `Result`,而不是只带成功结果。**

B 侧的**任何**失败(本地服务挂了、超时、请求已过期)都必须**显式回写一条错误响应**。否则 A 只能干等到超时:

- 白白浪费几秒延迟
- 无法区分「B 没收到」和「B 处理失败」

这是这类桥接方案最容易漏掉的一点。

### 2.3 编解码

使用 **bincode** 或 **MessagePack(rmp-serde)**。

**不要用 JSON** —— body 是二进制,JSON 装不下,会逼你做 base64,平白膨胀 33% 并增加 CPU 开销。Redis 的 value 本身是 binary-safe 的,直接塞原始字节即可。

### 2.4 Key 约定

```rust
/// 请求队列:所有 B 侧 agent 共同消费
pub fn request_queue(service: &str) -> String {
    format!("bridge:req:{service}")
}

/// 回复队列:每个 A 实例一个(注意不是每个请求一个,见 §3)
pub fn reply_queue(instance_id: &str) -> String {
    format!("bridge:resp:{instance_id}")
}
```

### 2.5 全局常量

```rust
pub const MAX_BODY_SIZE: usize = 4 * 1024 * 1024;  // 4 MB
pub const RESP_TTL_SECS: usize = 60;
```

---

## 3. client(A 侧)— 最关键的架构决策

### 3.1 两种做法的对比

| | 朴素做法 | **推荐做法** |
|---|---|---|
| 回复队列 | 每个请求一个 `bridge:resp:{req_id}` | 每个**实例**一个 `bridge:resp:{instance_id}` |
| 等待方式 | 每个请求 `BRPOP` 各自的队列 | 后台单任务统一 `BRPOP`,进程内路由 |
| 连接占用 | **N 个并发 = N 条阻塞连接** ❌ | **恒定 2 条连接** ✅ |

**朴素做法的致命问题**:`BRPOP` 会独占一条 Redis 连接。100 个并发请求就要 100 条连接阻塞着,连接池瞬间打爆。

### 3.2 推荐结构

```rust
use dashmap::DashMap;
use tokio::sync::oneshot;

pub struct BridgeClient {
    redis: deadpool_redis::Pool,
    instance_id: String,           // 本进程唯一 ID,启动时生成
    pending: Arc<DashMap<Uuid, oneshot::Sender<HttpResponse>>>,
}

impl BridgeClient {
    /// 内部 spawn reply_loop 后台任务
    pub async fn start(cfg: Config) -> Result<Self>;

    /// 对外的唯一入口
    pub async fn call(&self, req: HttpRequest) -> Result<HttpResponse>;
}

/// 后台单任务:一条专用连接,永远 BRPOP 本实例的回复队列
async fn reply_loop(
    conn: DedicatedConn,
    instance_id: String,
    pending: Arc<DashMap<Uuid, oneshot::Sender<HttpResponse>>>,
    shutdown: CancellationToken,
) {
    // BRPOP → 解码 → pending.remove(req_id) → oneshot.send(resp)
}
```

### 3.3 `call()` 的执行流程

1. 创建 `oneshot` channel,以 `req_id` 为 key 存入 `pending` map
2. `LPUSH` 请求到 `bridge:req:{service}`,`reply_to` 字段填本实例的 `instance_id`
3. `await` 那个 oneshot receiver,外面套 `tokio::time::timeout`
4. 后台 `reply_loop` 收到响应 → 查 map → 把结果 send 给对应的 oneshot

**收益**:无论多少并发,A 侧只占用 **1 条阻塞连接(reply_loop)+ 1 条普通连接池**。并发路由完全在进程内用 map 完成。这是整个设计里性价比最高的一处。

### 3.4 必须处理的清理逻辑

`call()` 超时后,**必须主动从 `pending` 里移除自己那一条**,否则 map 会持续泄漏。建议用 RAII guard 或 `tokio::select!` 的 else 分支保证移除一定执行。

---

## 4. agent(B 侧)— 拉取 → 转发 → 回写

### 4.1 结构

```rust
pub struct Agent {
    redis: deadpool_redis::Pool,
    http: reqwest::Client,             // 复用到 localhost 的连接池
    upstream: Url,                     // http://127.0.0.1:8080
    sem: Arc<Semaphore>,               // 并发上限
}
```

### 4.2 主循环

```rust
async fn pull_loop(agent: Arc<Agent>, shutdown: CancellationToken) {
    loop {
        // ① 先拿许可,再去拉 —— 顺序不能反
        let permit = agent.sem.clone().acquire_owned().await?;
        let raw = brpop(request_queue(&svc), 0).await?;
        tokio::spawn(handle_one(agent.clone(), raw, permit));
    }
}

async fn handle_one(
    agent: Arc<Agent>,
    raw: Bytes,
    _permit: OwnedSemaphorePermit,     // drop 时自动释放
) {
    let req = decode(raw);
    // 1. deadline 检查 —— 已过期直接回 Expired,不打本地服务
    // 2. header 白名单过滤后转发到 upstream
    // 3. 无论成败,都回写一条 HttpResponse
    // 4. 回写使用 pipeline:LPUSH + EXPIRE 合并为一次往返
}
```

### 4.3 三个要点

**① 先拿 semaphore,再 BRPOP —— 顺序不能反**

顺序反了就变成「拉进来再排队」,内存里堆积一堆请求。先拿许可等于告诉 Redis「我忙不过来,暂时别给我」,**队列本身自然成为缓冲区**,这就是背压(backpressure)。

**② deadline 先检查**

拉取到请求时,如果 A 早已超时离开,直接回 `Expired`,**不要去打本地服务**。避免无意义的负载,更重要的是避免重复副作用(A 可能已经重试了)。

**③ 回写用 pipeline + EXPIRE**

```
PIPELINE:
    LPUSH  bridge:resp:{reply_to}  <encoded>
    EXPIRE bridge:resp:{reply_to}  60
```

`EXPIRE` 是**防泄漏的保险丝**:如果 A 实例已经崩溃/下线,它的回复队列不会永远留在 Redis 里。

### 4.4 水平扩展

B 侧可以启动 N 个 agent 实例,共同 `BRPOP` 同一个请求队列 —— **Redis List 天然实现负载均衡**,无需额外协调。

---

## 5. 横切设计

### 5.1 优雅关闭

`CancellationToken` 传入 `pull_loop`:

1. 收到信号后**停止拉取新请求**
2. 等待 in-flight 的 `handle_one` 全部完成(semaphore 可用许可数回到满值即为排空)
3. 再退出进程

否则重启会丢掉正在处理中的请求。

### 5.2 Header 处理

**不要无脑透传所有 header。** 以下 hop-by-hop header 必须在 B 侧剔除或重写:

- `Host`(应改为 upstream 的 host)
- `Connection`
- `Content-Length`(由 reqwest 重新计算)
- `Transfer-Encoding`
- `Upgrade` / `Keep-Alive`

直接转发会导致 `reqwest` 行为异常或请求失败。

建议在 `protocol` 中维护一份白名单/黑名单常量,两侧共用。

### 5.3 Body 大小限制

在 `protocol` 中定义 `MAX_BODY_SIZE`:

- A 侧发送前检查
- B 侧回写前检查
- 超限返回 `BridgeError::TooLarge`

不加限制,迟早有人往 Redis 里塞几百 MB 把它打爆。

### 5.4 可观测性

至少埋这几个指标:

| 指标 | 说明 |
|---|---|
| **队列积压(`LLEN`)** | **最重要的健康指标** —— 一涨就说明 B 侧处理不过来或已挂掉 |
| 端到端延迟 | A 发出到收到响应的完整耗时 |
| `Expired` 计数 | 反映超时配置是否合理 |
| in-flight 并发数 | semaphore 占用情况 |
| Redis 重连次数 | 网络健康度 |

### 5.5 连接韧性

`reply_loop` 和 `pull_loop` 都必须能**自愈**:

- Redis 断线后按指数退避重连
- 不能一断线就整个进程失能
- 重连成功后继续消费,无需人工介入

---

## 6. 依赖建议

```toml
[dependencies]
redis = { version = "*", features = ["tokio-comp", "connection-manager"] }
deadpool-redis = "*"        # 连接池;阻塞命令需取专用连接
tokio = { version = "*", features = ["full"] }
tokio-util = "*"            # CancellationToken
reqwest = { version = "*", features = ["stream"] }
bincode = "*"               # 或 rmp-serde
bytes = "*"
uuid = { version = "*", features = ["v4", "serde"] }
dashmap = "*"
serde = { version = "*", features = ["derive"] }
tracing = "*"
```

---

## 7. 方案边界(务必知晓)

### 7.1 只支持 unary(一问一答)

**表达不了的场景**:

- SSE(Server-Sent Events)
- WebSocket
- Chunked streaming / 大文件流式传输

原因:Redis List 的语义是「一条消息一个整体」,无法表达持续的数据流。

如果 B 的服务包含流式接口,那部分只能另想办法(例如把流切成多条消息走 Redis Streams,但复杂度陡增,**不建议一开始就做**)。

### 7.2 延迟会显著增加

原本一次 HTTP RTT,现在变成至少 4 段:

```
A → Redis  +  Redis → B  +  B → 本地服务  +  回程
```

**上线前先实测两端到 Redis 的 RTT**,对延迟预算心里有数。

### 7.3 Redis 成为单点

Redis 挂掉 = 整条链路中断。原本的点对点直连至少还是独立的。需要在监控和容灾上考虑这一点。

### 7.4 可靠性:List 没有 ACK

当前设计用 Redis List,B 侧进程在处理途中崩溃,**那条请求就丢了**(A 侧会超时)。

如果需要「绝不丢请求」,升级路径是换成 **Redis Streams**:

| | List | Streams |
|---|---|---|
| 阻塞消费 | `BRPOP` | `XREADGROUP` |
| ACK 机制 | 无 | 有(`XACK`) |
| 崩溃重投 | 不支持 | 支持(pending list + `XCLAIM`) |
| 复杂度 | 低 | 中 |

**建议**:先用 List 跑通,观察一段时间的积压和失败率,确有需要再升级。升级后要注意**幂等性** —— 同一请求可能被执行两次,B 侧的写操作接口要么天然幂等,要么用 `req_id` 做去重。

---

## 8. 实施顺序建议

1. **`protocol` crate** —— 地基,定死之后 client 和 agent 可并行开工
2. **`agent`** —— 先能独立跑通「从队列取 → 打本地 HTTP → 回写」
3. **`client`** —— 实现 reply_loop + pending map 路由
4. **联调 + 埋点** —— 重点观察队列积压和端到端延迟
5. 视情况再考虑 Streams / 流式支持
