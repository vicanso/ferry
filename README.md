# ferry

把 Redis 当作反向隧道的传输层,让网络不通的两台机器之间跑通 HTTP 调用。

A 机器调不到 B 机器的 HTTP 服务,但两边都能连同一个 Redis。ferry 反转连接方向:
B 主动去 Redis 拉任务,两端都是出向连接,绕开 A 无法直连 B 的限制。

设计文档见 [design.md](design.md),线上协议(跨语言实现对照)见 [PROTOCOL.md](PROTOCOL.md)。

```
A(调用方)                  Redis                    B(有 HTTP 服务)
   |                         |                          |
   | LPUSH 请求 ------------> │  bridge:req:{service}    |
   |                         │ <----- BRPOP 阻塞拉取      |
   |                         │                          | 转发到 127.0.0.1:8080
   | 收到响应 <-------------- │  bridge:resp:{instance}  |
```

## Workspace

| crate | 位置 | 职责 |
|---|---|---|
| `bridge-protocol` | `crates/protocol` | 消息定义、JSON 编解码、key 约定、hop-by-hop 黑名单 |
| `bridge-client` | `crates/client` | A 侧,对外暴露 `BridgeClient::call()` |
| `bridge-agent` | `crates/agent` | B 侧,拉取 → 转发本地 HTTP → 回写(库 + 独立 bin) |
| `ferry-git` | `crates/git` | git 操作库:切分支 / fetch / pull / 取两个 commit 的 patch(库 + CLI) |
| `ferry-git-api` | `crates/git-api` | 上面那套的 HTTP 外壳,`GET /patch`(库 + 独立 bin) |
| `ferry` | `crates/ferry` | **部署用的合并二进制**:agent 与 git-patch 服务同进程 |

协议独立成 crate,让两个独立部署的二进制在编译期锁死协议一致性。

镜像里实际跑的是 `ferry`。两个子系统**按配置各自启用**:配了 `agent.upstreams` 就跑
agent,配了 `server.root` 就跑 git-patch 服务,都没配则拒绝启动(退出码 1)。原有的
纯 agent 部署换成它**无需改任何环境变量**。

## 运行

B 侧(可启动多个实例,Redis List 天然负载均衡):

```bash
FERRY__REDIS__URI=redis://127.0.0.1:6379 \
FERRY__AGENT__SERVICE=demo \
FERRY__AGENT__UPSTREAMS=grok=http://127.0.0.1:8080,api=http://127.0.0.1:9090/api \
cargo run -p bridge-agent
```

配置分三层叠加,后者覆盖前者:

1. 烘焙进二进制的 `crates/agent/config/default.toml`
2. `FERRY_CONFIG` 指向的外部 TOML(可选)
3. 环境变量,前缀 `FERRY`,层级分隔符是**双下划线**

容器里通常什么都不挂,纯环境变量即可;需要复杂配置时再挂文件。

| 配置项 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `redis.uri` | `FERRY__REDIS__URI` | `redis://127.0.0.1:6379` | 支持聚合写法,密码只写一次 |
| `agent.service` | `FERRY__AGENT__SERVICE` | `demo` | 消费哪个服务的队列 |
| `agent.key_prefix` | `FERRY__AGENT__KEY_PREFIX` | `bridge` | Redis key 前缀,**须与 A 侧一致** |
| `agent.upstreams` | `FERRY__AGENT__UPSTREAMS` | *(空,拒绝启动)* | 服务名 → 真实上游(base URL,可选注入 header) |
| `agent.max_concurrency` | `FERRY__AGENT__MAX_CONCURRENCY` | `64` | 在途请求上限 |
| `agent.brpop_timeout_secs` | `FERRY__AGENT__BRPOP_TIMEOUT_SECS` | **`2`** | 请求队列 BRPOP 最长阻塞秒数(≥1)。调大可降低空闲 Redis 命令数,代价是关闭/断线感知变慢 |
| `agent.metrics_interval_secs` | `FERRY__AGENT__METRICS_INTERVAL_SECS` | **`30`** | 指标 `LLEN` 间隔秒数;`0` 关闭 metrics 循环 |

调用方只在消息里写**逻辑地址** `https://{服务名}/path`,服务名对应的真实 host 由
`agent.upstreams` 指定 —— 这样 Redis 里只出现服务名,真实上游地址不外泄,调用方也无法
指向任意内网地址。配置文件里 `[agent.upstreams]` 写成表,环境变量用单个变量带整表
(`grok=http://...,api=http://...`,逗号分隔)。服务名不在映射内一律拒绝;**留空则拒绝启动**。

每个服务还能配**注入 header**:转发到该上游时附加,并**覆盖调用方的同名 header**
(config wins)。适合放调用方不该经手的凭证(`Authorization`、内网签名头等)—— 这样连
密钥都不进 Redis。配置文件里写 `[agent.upstreams.<服务>.headers]` 子表;环境变量用嵌套写法
`FERRY__AGENT__UPSTREAMS__<服务>__HEADERS__<NAME>=...`(带 header 时不能再用逗号简写,
两种形状不同)。注入的 header 值在启动时即校验,非法立即报错,且不会进日志。

```toml
[agent.upstreams.grok]
base = "http://10.51.0.5:7257"
[agent.upstreams.grok.headers]
authorization = "Bearer sk-..."   # 调用方不必再发,Redis 里也看不到
```

Redis 连接池参数(`pool_size`、`connection_timeout` 等)写在 URI 的查询串里,
由 tibba-cache 解析:

```
redis://:pw@h1:6379,h2:6379/?pool_size=20&connection_timeout=3s
```

或用容器跑(多阶段构建,产出合并二进制 `ferry`;client 是库,由调用方自己集成):

```bash
docker build -t ferry .

# agent 要访问宿主机上的本地 HTTP 服务,所以用 host 网络最省事
docker run --rm --network host \
  -e FERRY__REDIS__URI=redis://127.0.0.1:6379 \
  -e FERRY__AGENT__SERVICE=demo \
  -e FERRY__AGENT__UPSTREAMS=grok=http://127.0.0.1:8080 \
  ferry
```

停止时用 `docker stop`(发 SIGTERM),两个子系统一起排空 —— agent 停止拉取新请求、
HTTP 服务停止收新连接,各自等在途请求做完再退出。**grace period 要留够** —— `docker stop` 默认只给 10 秒,
k8s 用 `terminationGracePeriodSeconds`;时间不够会在排空途中被 SIGKILL。

A 侧:

```rust
let client = BridgeClient::start(
    Config::new("redis://127.0.0.1:6379", "demo")
        // 可选:回复队列 BRPOP 超时,默认 2s;省 Upstash 等免费命令额度时可调大
        // .with_brpop_timeout(Duration::from_secs(30))
).await?;
let resp = client.call(CallRequest {
    method: "GET".into(),
    // 逻辑地址:host 是服务名 grok,真实 host 由 agent 的 upstreams 配置决定
    url: "https://grok/api/foo?x=1".into(),
    headers: vec![("accept".into(), "*/*".into())],
    body: Bytes::new(),
    timeout: Duration::from_secs(10),
}).await?;
```

A 侧 `Config` 字段与默认值:

| 字段 | 默认值 | 说明 |
|---|---|---|
| `redis_url` | *(必填)* | Redis URI |
| `service` | *(必填)* | 目标服务名 |
| `key_prefix` | `bridge` | 须与 agent 一致;`Config::with_key_prefix` |
| `brpop_timeout` | **`2s`** (`DEFAULT_BRPOP_TIMEOUT`) | 回复队列 BRPOP 阻塞时长;≥1s;`Config::with_brpop_timeout` |

### 降低云 Redis 空闲命令数(可接受更高延迟)

空闲时 agent 的 pull 循环和 client 的 `reply_loop` 仍会周期性发 `BRPOP`(超时返回 nil
也计一次命令)。**默认每 2 秒一次**。若业务不要求实时、可接受更高延迟,两端一起调大:

```bash
# agent:BRPOP 30s 一次;metrics 5 分钟一次(或 0 关掉)
FERRY__AGENT__BRPOP_TIMEOUT_SECS=30 \
FERRY__AGENT__METRICS_INTERVAL_SECS=300 \
FERRY__REDIS__URI=... FERRY__AGENT__SERVICE=demo \
FERRY__AGENT__UPSTREAMS=grok=http://127.0.0.1:8080 \
cargo run -p bridge-agent
```

```rust
// client:与 agent 独立配置,但建议同量级,否则省命令效果不对齐
BridgeClient::start(
    Config::new(redis_url, service)
        .with_brpop_timeout(Duration::from_secs(30)),
).await?;
```

粗算:BRPOP 从 2s → 30s,单进程空闲命令数约降 **15 倍**。单次有消息时的路径仍是
`LPUSH`/`BRPOP`/`LPUSH+EXPIRE`,与该超时无关。

两个可运行示例:

```bash
cargo run -p bridge-client --example demo -- /api/foo        # 单次调用
cargo run -p bridge-client --example concurrent -- 500 /     # 并发冒烟测试
```

## Git patch 服务

同一个 `ferry` 进程里还能跑一个 git patch 服务:给定仓库、分支和两个 commit,返回
它们之间的完整 patch。仓库放在一个根目录下 —— 可以预先克隆,也可以配白名单让它按需
自动克隆(见下文)。

```bash
FERRY__SERVER__ROOT=/srv/repos cargo run -p ferry
```

| 配置项 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `server.root` | `FERRY__SERVER__ROOT` | *(空,不启用)* | 仓库根目录;`repo` 参数在此目录下解析 |
| `server.addr` | `FERRY__SERVER__ADDR` | `127.0.0.1:7100` | 监听地址,**默认只绑回环** |
| `server.remote` | `FERRY__SERVER__REMOTE` | `origin` | fetch 用的远端名 |
| `server.repos` | `FERRY__SERVER__REPOS__<名字>` | *(空,不自动克隆)* | 允许自动克隆的白名单:仓库名 → clone URL |
| `server.max_concurrency` | `FERRY__SERVER__MAX_CONCURRENCY` | `8` | 同时进行的 git 操作数(git 调用是阻塞的) |
| `server.max_patch_bytes` | `FERRY__SERVER__MAX_PATCH_BYTES` | **`2097152`**(2 MiB) | 超限返回 413 而非截断 |

```
GET /patch?repo=<name>&branch=<branch>&prevCommit=<sha>&currentCommit=<sha>
GET /health
```

```json
{
  "repo": "ferry", "branch": "main",
  "prevCommit": "abc1234...", "currentCommit": "def5678...",
  "fetched": true, "cloned": false,
  "files": ["crates/agent/src/main.rs"],
  "insertions": 19, "deletions": 2,
  "patch": "diff --git a/crates/agent/src/main.rs ..."
}
```

短 sha 可用,响应回显补全后的完整 sha。**只 fetch、不 checkout**:`git diff A B` 是纯
对象库操作,不需要工作区 —— 于是并发请求同一仓库的不同分支不会互相踩,目录被人改脏
或本地分叉也不影响。两个 commit 本地都有时连 fetch 都跳过(`fetched: false`),缺哪个
才拉一次该分支。

### 仓库从哪来

两种方式,可以混用。

**预先克隆**到根目录里。既然不需要工作区,`--mirror` 比普通克隆省:

```bash
cd /srv/repos
git clone --mirror https://git.example.com/team/ferry.git ferry
```

**或者配白名单,首次请求时自动克隆**(同样是 bare,不检出工作区):

```toml
[server.repos]
ferry = "https://git.example.com/team/ferry.git"
# ssh 也支持,用 scp 式写法:git@git.example.com:team/ferry.git
```

```bash
FERRY__SERVER__REPOS__FERRY=https://git.example.com/team/ferry.git
```

**只认白名单。** 请求里给的是**名字**,URL 完全由配置决定 —— 调用方碰不到地址,名字
不在表里直接 404,服务绝不会拿请求里的东西去拼 URL 连外部地址。这与 `agent.upstreams`
是同一条边界。留空(默认)则不自动克隆,仓库必须预先放好。克隆的认证与 fetch 共用一套。

响应里的 `cloned` / `fetched` 标明这次请求是否真的动了网络:首次克隆后两者分别为
`true` / `false`,之后命中本地对象则都是 `false`,只有缺 commit 时才 `fetched: true`。

### 经 Redis 取 patch

服务默认只绑回环,不直接对外。把它注册成 agent 的一个 upstream,调用方就用同一个
`BridgeClient`、同一套队列约定拿到 patch —— 这正是 agent 的本职:把本地 HTTP 服务经
Redis 暴露出去,不必为它另写一套 Redis 消费逻辑。

```bash
-e FERRY__SERVER__ROOT=/srv/repos
-e FERRY__AGENT__UPSTREAMS__GITPATCH__BASE=http://127.0.0.1:7100
```

```rust
let resp = client.call(CallRequest {
    method: "GET".into(),
    // host 是服务名 gitpatch,真实地址由 upstreams 配置决定
    url: "http://gitpatch/patch?repo=ferry&branch=main\
          &prevCommit=abc1234&currentCommit=def5678".into(),
    headers: vec![],
    body: Bytes::new(),
    timeout: Duration::from_secs(30),
}).await?;
```

**同进程是这里的关键。** 拆成两个容器时 `127.0.0.1` 不通,还得额外规划容器网络;同
进程后 agent 直连回环即可,而 git patch 服务不必对外监听,入口唯一收敛到 Redis。

**注意 `max_patch_bytes` 与协议上限的联动。** 响应要过 bridge 协议的 4 MiB
`MAX_BODY_SIZE`,而 patch 装进 JSON 会因转义膨胀(换行 1 字节变 2,patch 里换行极
密集,最坏接近翻倍)。默认取 2 MiB 正是为了转义后仍在 4 MiB 以内 —— 调大了会出现
「服务自己放行、却在 agent 那关被 `TooLarge` 拒掉」这种定位困难的失败。只走本地
HTTP、不过 Redis 时可以调大。

### 安全边界

`repo` 来自请求,直接拼进路径就是任意目录读取。两道拦截:先按语法拒绝(空段、`.`、
`..`、反斜杠),再 `canonicalize` 后确认仍在根目录内 —— 后者才挡得住根目录里指向外部
的符号链接,光看字符串是看不出来的。越界返回 400,仓库不存在返回 404。

`prevCommit` / `currentCommit` 交给 libgit2 的 revparse,没有命令注入面:全程调库,
不 fork shell。fetch 的认证走 ssh-agent / git credential helper,也就是用户 `git fetch`
本来在用的那套;容器里通常没有 ssh-agent,建议 remote 用 https + credential helper,
或把只读部署密钥挂进来。

镜像为此装了 `libssl3` + `ca-certificates`(libgit2 是 C 库,不认 rustls,这是全项目
唯一让 openssl 进来的地方);没装 `git`,fetch 由 libgit2 自己实现协议。

### 命令行

同一套能力也有 CLI,用于本机排查:

```bash
cargo run -p ferry-git -- -C /srv/repos/ferry diff HEAD~5 HEAD > changes.patch
cargo run -p ferry-git -- -C /srv/repos/ferry checkout main
cargo run -p ferry-git -- -C /srv/repos/ferry pull
```

`checkout` 用 libgit2 的 SAFE 策略、**绝不 force**,会被覆盖的未提交改动会让它失败并
列出挡路的文件;`pull` **只做 fast-forward**,分叉时报错而不是自动 merge 或
`reset --hard`。

## 几处关键取舍

**回复队列按实例划分,不按请求划分。** 每个请求一个队列意味着每个并发请求独占一条
`BRPOP` 阻塞连接,100 并发就是 100 条连接。ferry 让后台单任务 `reply_loop` 用一条专用
连接消费本实例的回复队列,响应靠 `req_id` 在进程内路由回各自的调用者。实测 500 并发只
增加约 26 条 Redis 连接(1 条阻塞 + 有界连接池),而非 500 条。这是 `reply_mode` 的默认
值 `queue`,适合同步等待、进程常驻的调用方(`bridge-client` 走这条)。

**另有 `reply_mode: kv`,给「发完先走、之后再来取」的调用方。** 响应不进队列,而是
`SET bridge:resp:{req_id}` 并带 TTL(默认 600s / 10 分钟),调用方之后按 `req_id` `GET` / `GETDEL`
自取 —— 发请求的进程可以退出,换个进程 / 隔段时间再来拿。代价是没有阻塞唤醒,要主动轮询
来取,且过了 TTL 窗口响应就被回收。为什么用一个 reqId 一个 string key 而不是一个大 hash:
string 的 `SET … EX` 每条响应独立过期、自动清理,且 `bridge:resp:{req_id}` 在 cluster 下
散列到不同 slot、负载均摊,而单个 hash 全落一个 slot 是热点 key、还得靠 `HEXPIRE` 逐字段过期。

**B 侧任何失败都显式回写错误响应。** 本地服务挂了、超时、请求已过期,都回一条
`BridgeError`。否则 A 只能干等到超时,且无法区分「B 没收到」和「B 处理失败」。

**调用方给服务名,真实地址在 agent 侧。** 消息里的 `url` 是逻辑地址 `https://{服务名}/path`,
agent 拿服务名查 `agent.upstreams` 得到真实 base URL,再拼上请求的 path/query 去调用;
scheme/host/port 全部来自配置,调用方碰不到。这条边界决定了 ferry 是「通向若干指定服务的
桥」而不是「通向 B 内网的隧道」—— 真实上游地址既不进 Redis,调用方也无法把 agent 指向
任意内网地址(比如云元数据服务)。服务名不在映射内一律拒绝、不发任何网络请求,并**禁用
自动重定向**,免得上游用一个 302 把 agent 骗进内网。若映射值带 base path,则请求被限制在
该子树内(拒绝 `%2e`/`%2f` 编码穿越)。每个服务还可在配置里指定**注入 header**(转发时
覆盖调用方同名),把 `Authorization` 之类凭证也留在 B 侧配置 —— 连密钥都不必进 Redis。

**先拿 semaphore 许可,再 BRPOP。** 顺序反了就变成「拉进来再排队」,请求堆在进程内存里。
先拿许可等于告诉 Redis「我忙不过来,暂时别给我」,队列本身成为缓冲区,这就是背压。

**BRPOP 一旦发出就不能取消。** 命令送达 Redis 后元素已从 list 弹出,此时丢弃 future
等于把这条请求扔掉 —— 它既不在队列里也没人处理,调用方只能干等超时。所以关闭信号只在
两次 BRPOP 之间检查,代价是关闭最多多等 `brpop_timeout`(**默认 2 秒**,可配)。

**回写用 pipeline 合并 `LPUSH` + `EXPIRE`。** `EXPIRE` 是防泄漏保险丝:A 实例崩溃后
它的回复队列不会永远留在 Redis 里。

**线上格式是 JSON。** 选 JSON 不是为了性能(它比 MessagePack 大 31%),而是为了跨语言
和可调试 —— 出问题时能直接 `redis-cli LRANGE bridge:req:demo 0 -1` 看到 method、URL、
headers。body 的编码请求和响应**刻意不同**:

- **请求和响应 body 都按内容二选一**,用同级的 `body_encoding` 标注:文本(合法 UTF-8,
  比如 JSON)直接存原文,`redis-cli` 直读、可直接落盘;含非 UTF-8 字节才 base64(标准
  字母表 + 填充)。直接把字节交给 serde_json 会变成数字数组,膨胀 3.1 倍且不可读,所以
  二进制仍需显式 base64。
- **响应的压缩由 agent 透明解开** —— 它剥掉调用方的 `accept-encoding`,自行向上游协商
  gzip / brotli 并解压,回来的 body 已是明文,所以文本响应几乎总是原文可读。

## 方案边界

- **只支持一问一答**,表达不了 SSE / WebSocket / chunked streaming。
- **延迟显著增加**,一次 HTTP RTT 变成至少 4 段。上线前先实测两端到 Redis 的 RTT。
- **Redis 是单点**,挂掉即整条链路中断。
- **List 没有 ACK**,agent 处理途中崩溃那条请求就丢了(A 侧超时)。需要「绝不丢请求」
  则要换 Redis Streams(`XREADGROUP` + `XACK` + `XCLAIM`),同时得处理幂等性。

## 可观测性

agent 按 `metrics_interval_secs`(**默认 30 秒**;`0` 关闭)输出一次 `backlog`(请求队列
`LLEN`,最重要的健康指标 —— 一涨就说明 B 侧处理不过来或已挂掉)和 `in_flight`
(semaphore 占用)。

排查报文用 `scripts/ferry-dump.py`,它会自动把 body 解开 base64,二进制 body
显示成十六进制预览并标注类型:

```bash
scripts/ferry-dump.py req demo           # 看请求队列积压
scripts/ferry-dump.py resp <instance_id> # 看某个实例的回复队列
```
