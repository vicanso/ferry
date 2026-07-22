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
| `bridge-agent` | `crates/agent` | B 侧二进制,拉取 → 转发本地 HTTP → 回写 |

协议独立成 crate,让两个独立部署的二进制在编译期锁死协议一致性。

## 运行

B 侧(可启动多个实例,Redis List 天然负载均衡):

```bash
REDIS_URL=redis://127.0.0.1:6379 \
BRIDGE_SERVICE=demo \
UPSTREAM_URL=http://127.0.0.1:8080 \
MAX_CONCURRENCY=64 \
cargo run -p bridge-agent
```

或用容器跑(多阶段构建,只产出 agent;client 是库,由调用方自己集成):

```bash
docker build -t ferry-agent .

# agent 要访问宿主机上的本地 HTTP 服务,所以用 host 网络最省事
docker run --rm --network host \
  -e REDIS_URL=redis://127.0.0.1:6379 \
  -e BRIDGE_SERVICE=demo \
  -e UPSTREAM_URL=http://127.0.0.1:8080 \
  -e MAX_CONCURRENCY=64 \
  ferry-agent
```

停止时用 `docker stop`(发 SIGTERM),agent 会停止拉取新请求并等 in-flight
排空后再退出。**grace period 要留够** —— `docker stop` 默认只给 10 秒,
k8s 用 `terminationGracePeriodSeconds`;时间不够会在排空途中被 SIGKILL。

A 侧:

```rust
let client = BridgeClient::start(Config::new("redis://127.0.0.1:6379", "demo")).await?;
let resp = client.call(CallRequest {
    method: "GET".into(),
    uri: "/api/foo?x=1".into(),
    headers: vec![("accept".into(), "*/*".into())],
    body: Bytes::new(),
    timeout: Duration::from_secs(10),
}).await?;
```

两个可运行示例:

```bash
cargo run -p bridge-client --example demo -- /api/foo        # 单次调用
cargo run -p bridge-client --example concurrent -- 500 /     # 并发冒烟测试
```

## 几处关键取舍

**回复队列按实例划分,不按请求划分。** 每个请求一个队列意味着每个并发请求独占一条
`BRPOP` 阻塞连接,100 并发就是 100 条连接。ferry 让后台单任务 `reply_loop` 用一条专用
连接消费本实例的回复队列,响应靠 `req_id` 在进程内路由回各自的调用者。实测 500 并发只
增加约 26 条 Redis 连接(1 条阻塞 + 有界连接池),而非 500 条。

**B 侧任何失败都显式回写错误响应。** 本地服务挂了、超时、请求已过期,都回一条
`BridgeError`。否则 A 只能干等到超时,且无法区分「B 没收到」和「B 处理失败」。

**先拿 semaphore 许可,再 BRPOP。** 顺序反了就变成「拉进来再排队」,请求堆在进程内存里。
先拿许可等于告诉 Redis「我忙不过来,暂时别给我」,队列本身成为缓冲区,这就是背压。

**BRPOP 一旦发出就不能取消。** 命令送达 Redis 后元素已从 list 弹出,此时丢弃 future
等于把这条请求扔掉 —— 它既不在队列里也没人处理,调用方只能干等超时。所以关闭信号只在
两次 BRPOP 之间检查,代价是关闭最多多等 `BRPOP_TIMEOUT_SECS`(2 秒)。

**回写用 pipeline 合并 `LPUSH` + `EXPIRE`。** `EXPIRE` 是防泄漏保险丝:A 实例崩溃后
它的回复队列不会永远留在 Redis 里。

**线上格式是 JSON,body 走 base64。** 选 JSON 不是为了性能(它比 MessagePack 大 31%),
而是为了跨语言和可调试 —— 出问题时能直接 `redis-cli LRANGE bridge:req:demo 0 -1` 看到
method、URI、headers。body 必须显式 base64:直接把字节交给 serde_json 会序列化成数字
数组,膨胀 3.1 倍且完全不可读。base64 用标准字母表加填充,与 Go `encoding/json` 处理
`[]byte` 的方式一致,Go 侧声明成 `[]byte` 即可自动对齐。

## 方案边界

- **只支持一问一答**,表达不了 SSE / WebSocket / chunked streaming。
- **延迟显著增加**,一次 HTTP RTT 变成至少 4 段。上线前先实测两端到 Redis 的 RTT。
- **Redis 是单点**,挂掉即整条链路中断。
- **List 没有 ACK**,agent 处理途中崩溃那条请求就丢了(A 侧超时)。需要「绝不丢请求」
  则要换 Redis Streams(`XREADGROUP` + `XACK` + `XCLAIM`),同时得处理幂等性。

## 可观测性

agent 每 30 秒输出一次 `backlog`(请求队列 `LLEN`,最重要的健康指标 —— 一涨就说明
B 侧处理不过来或已挂掉)和 `in_flight`(semaphore 占用)。

排查报文用 `scripts/ferry-dump.py`,它会自动把 body 解开 base64,二进制 body
显示成十六进制预览并标注类型:

```bash
scripts/ferry-dump.py req demo           # 看请求队列积压
scripts/ferry-dump.py resp <instance_id> # 看某个实例的回复队列
```
