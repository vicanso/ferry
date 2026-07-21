# ferry 线上协议

跨语言实现(Go 等)按本文对照即可。Rust 侧的权威定义在 `crates/protocol/src/lib.rs`,
本文档描述的是它序列化后的实际形态。

## 传输

| 项 | 约定 |
|---|---|
| 编码 | JSON(UTF-8) |
| 请求队列 | `bridge:req:{service}`,A 侧 `LPUSH`,B 侧 `BRPOP` |
| 回复队列 | `bridge:resp:{instance_id}`,B 侧 `LPUSH` + `EXPIRE 60`,A 侧 `BRPOP` |
| 关联方式 | `req_id`,A 侧靠它把响应路由回对应的调用者 |
| 时间 | `deadline` 是 Unix **毫秒**绝对时间 |
| 二进制 | body 走 base64(标准字母表 + 填充),与 Go `encoding/json` 对 `[]byte` 的处理一致 |

回复队列**按实例划分,不是按请求划分**。每个进程启动时生成一个 `instance_id`,
全程只用一条连接 `BRPOP` 自己那个队列,收到后在进程内按 `req_id` 分发。
不要给每个请求建一个队列 —— 那样每个并发请求都会独占一条阻塞连接。

## 请求

```json
{
  "req_id": "11111111-2222-3333-4444-555555555555",
  "reply_to": "9ec3e780ba7c4745a1bd27702e0b44d9",
  "method": "POST",
  "uri": "/api/orders?page=1",
  "headers": [["content-type", "application/json"], ["x-from", "go"]],
  "body": "eyJwaW5nIjp0cnVlfQ==",
  "deadline": 1784644275536
}
```

- `reply_to` 填**发送方自己的 `instance_id`**,B 侧据此拼出 `bridge:resp:{reply_to}`。
- `headers` 是二元数组的数组,不是 object —— 同名 header 可以重复出现,object 表达不了。
- `body` 为空时用 `""`;`null` 也接受(Go 的 `json.Marshal` 会把 nil `[]byte` 编成 `null`)。
- **body 一律 base64,即使内容本身就是 JSON。** 响应 body 不受调用方控制:只要转发了
  `accept-encoding: gzip`,上游就可能返回 gzip 字节,那不是合法 UTF-8,存不进 JSON
  字符串。base64 正是这个桥能透明代理任意 `content-encoding` 的前提。
  排查时用 `scripts/ferry-dump.py` 还原可读形式。
- `deadline` 已过期的请求,B 侧直接回 `Expired`,**不会**打本地服务。

## 响应

成功:

```json
{
  "req_id": "11111111-2222-3333-4444-555555555555",
  "result": {
    "Ok": {
      "status": 200,
      "headers": [["content-type", "application/json"]],
      "body": "eyJwaW5nIjp0cnVlfQ=="
    }
  }
}
```

失败:

```json
{
  "req_id": "11111111-2222-3333-4444-555555555555",
  "result": { "Err": { "type": "UpstreamTimeout" } }
}
```

`result` 是二选一的外部标签,只会出现 `Ok` 或 `Err` 之一。

**B 侧任何失败都必须显式回写一条 `Err`。** 不回写的话,调用方只能干等到超时,
而且无法区分「B 没收到」和「B 处理失败」。这是这类桥接方案最容易漏掉的一点。

### 错误变体

每个变体都是带 `type` 字段的对象,无字段的变体也不例外 —— 这样解析端不用为
「有时是字符串有时是对象」写特例。

| `type` | 附加字段 | 含义 |
|---|---|---|
| `UpstreamUnreachable` | `detail: string` | B 连不上本地服务 |
| `UpstreamTimeout` | — | 本地服务在 deadline 内没返回 |
| `Expired` | — | 拉取到时已超过 deadline |
| `TooLarge` | `limit: number` | body 超出大小限制 |
| `Internal` | `detail: string` | 其他内部错误 |

## 限制

- `body` 上限 4 MiB,校验的是 **base64 之前的原始字节数**。发送前和回写前都要检查。
- 以下 hop-by-hop header 在转发前必须剔除,透传会导致 HTTP 客户端行为异常:
  `host`、`connection`、`content-length`、`transfer-encoding`、`upgrade`、
  `keep-alive`、`proxy-authenticate`、`proxy-authorization`、`te`、`trailer`。
- header value 用 string 表示。HTTP 规范允许任意字节,实践中都是 ASCII;
  非 UTF-8 的值会被 lossy 转换。
- 只支持一问一答,表达不了 SSE / WebSocket / chunked streaming。

## Go 参考结构

`[]byte` 字段由 `encoding/json` 自动做 base64,无需手工转换。

```go
type Request struct {
	ReqID    string      `json:"req_id"`
	ReplyTo  string      `json:"reply_to"`
	Method   string      `json:"method"`
	URI      string      `json:"uri"`
	Headers  [][2]string `json:"headers"`
	Body     []byte      `json:"body"`
	Deadline int64       `json:"deadline"`
}

type Response struct {
	ReqID  string `json:"req_id"`
	Result struct {
		Ok  *Ok          `json:"Ok,omitempty"`
		Err *BridgeError `json:"Err,omitempty"`
	} `json:"result"`
}

type Ok struct {
	Status  int         `json:"status"`
	Headers [][2]string `json:"headers"`
	Body    []byte      `json:"body"`
}

type BridgeError struct {
	Type   string `json:"type"`
	Detail string `json:"detail,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}
```

## 实现新的调用方时要注意

如果要用 Go 重写 A 侧,以下几点是这个设计里最容易出错的地方:

1. **`BRPOP` 发出后不能取消。** 命令送达 Redis 后元素就已从 list 弹出,此时若因
   `context` 取消而放弃读取结果,这条消息会彻底消失 —— 既不在队列里也没人处理,
   对端只能干等到超时。关闭信号只能在两次 `BRPOP` 之间检查。
   这个 bug 在本项目的 Rust 实现里真实出现过。
2. **超时后必须清理 pending 表**,否则内存持续泄漏。
3. **B 侧要先拿并发许可再 `BRPOP`**,顺序反了就变成「拉进来再排队」,
   请求堆在进程内存里而不是 Redis 队列里,失去背压。
4. **回写响应要带 `EXPIRE`**,否则调用方实例崩溃后,它的回复队列会永远留在 Redis 里。
