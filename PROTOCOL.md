# ferry 线上协议

跨语言实现(Go 等)按本文对照即可。Rust 侧的权威定义在 `crates/protocol/src/lib.rs`,
本文档描述的是它序列化后的实际形态。

## 传输

| 项 | 约定 |
|---|---|
| 编码 | JSON(UTF-8) |
| 请求队列 | `bridge:req:{service}`,A 侧 `LPUSH`,B 侧 `BRPOP` |
| 回复(队列模式) | `bridge:resp:{reply_to}`(list),B 侧 `LPUSH` + `EXPIRE 60`,A 侧 `BRPOP` 阻塞取 |
| 回复(KV 模式) | `bridge:resp:{req_id}`(string),B 侧 `SET … EX 600`,A 侧之后 `GET` / `GETDEL` 自取 |
| 关联方式 | `req_id`,A 侧靠它把响应对应回请求 |
| 时间 | `deadline` 是 Unix **毫秒**绝对时间 |
| body 编码 | 文本(合法 UTF-8)存原文、二进制走 base64,用同级 `body_encoding` 标注;请求响应同规则,base64 用标准字母表 + 填充 |

响应投递方式由请求的 `reply_mode` 决定:

- **`queue`(默认)** —— 回复队列**按实例划分,不按请求划分**。每个进程启动时生成一个
  `instance_id` 填进 `reply_to`,全程只用一条连接 `BRPOP` 自己那个队列,收到后在进程内按
  `req_id` 分发。**不要给每个请求建一个队列** —— 那样每个并发请求都独占一条阻塞连接
  (100 并发 = 100 条)。适合同步等待、进程常驻的调用方。
- **`kv`** —— 响应 `SET` 到 `bridge:resp:{req_id}` 并设 TTL,调用方之后按 `req_id`
  `GET` / `GETDEL` 自取。适合「发完先走、换进程 / 换时间再来取」。代价是没有阻塞唤醒,
  要主动来取,且响应只在 TTL 窗口内(默认 600s / 10 分钟)有效,过期得到 nil。此模式忽略 `reply_to`。

## 请求

```json
{
  "req_id": "11111111-2222-3333-4444-555555555555",
  "reply_to": "9ec3e780ba7c4745a1bd27702e0b44d9",
  "reply_mode": "queue",
  "method": "POST",
  "url": "http://10.0.0.5:8080/api/orders?page=1",
  "headers": [["content-type", "application/json"], ["x-from", "go"]],
  "body_encoding": "utf8",
  "body": "{\"ping\":true}",
  "deadline": 1784644275536
}
```

- `reply_mode` 选 `queue`(默认)或 `kv`,决定响应投递到哪、怎么取(见「传输」)。缺字段按
  `queue`,兼容不认识该字段的旧调用方。
- `reply_to`:**仅 `queue` 模式用**,填发送方自己的 `instance_id`,B 侧据此拼出
  `bridge:resp:{reply_to}`。`kv` 模式忽略它(响应按 `req_id` 寻址),填 `""` 即可。
- `url` 必须是**完整的绝对 URL**,不是路径。目标地址由调用方给出,但 agent 会拿它
  比对自己配置的 `ALLOWED_UPSTREAMS` 清单,不在清单内一律返回
  `UpstreamNotAllowed` 且**不会发出任何网络请求**。决定权始终在 B 侧 —— 否则任何
  能往队列 LPUSH 的人都能让 agent 访问 B 内网的任意地址(内部后台、数据库、
  云元数据服务 `169.254.169.254` 等)。
- 匹配规则:origin(scheme + host + port)必须完全一致;清单项若带路径,则目标
  路径须落在其子树内。agent 同时**禁用了自动重定向**,上游返回的 3xx 原样回传给
  调用方,不会被 agent 跟随 —— 否则一个指向内网的 302 就能绕过清单。
- `headers` 是二元数组的数组,不是 object —— 同名 header 可以重复出现,object 表达不了。
- **body 按内容选编码**,与响应同规则,靠同级的 `body_encoding` 区分:
  - `utf8` —— body 是合法 UTF-8(JSON、文本),字段里就是**原文**。手工往队列塞请求时
    直接写 JSON 原文即可(注意它作为外层 JSON 的字符串值,内部引号仍需转义,用
    `jq` / `python` 之类构造最省事)。
  - `base64` —— body 含非 UTF-8 字节(protobuf、二进制上传等),字段里是标准 base64。
  缺 `body_encoding` 按 `utf8` 解释;空 body 用 `""` 或 `null`(Go 的 nil `[]byte` → `null`)。
- `deadline` 已过期的请求,B 侧直接回 `Expired`,**不会**打本地服务。

## 响应

响应报文(下面这个 JSON)不变,变的只是它**落在哪个 key、怎么取**,由请求的
`reply_mode` 决定:`queue` → `LPUSH bridge:resp:{reply_to}`,`BRPOP` 取;`kv` →
`SET bridge:resp:{req_id}`,`GET` / `GETDEL` 取(见「传输」)。两种都在报文里带 `req_id`。

成功:

```json
{
  "req_id": "11111111-2222-3333-4444-555555555555",
  "result": {
    "Ok": {
      "status": 200,
      "headers": [["content-type", "application/json"]],
      "body_encoding": "utf8",
      "body": "{\"ping\":true}"
    }
  }
}
```

- **响应 body 按内容选编码**,靠同级的 `body_encoding` 区分:
  - `utf8` —— body 是合法 UTF-8(JSON、HTML、纯文本),字段里就是**原文**,`redis-cli`
    可直读、可直接落盘,不用再解一层。
  - `base64` —— body 含非 UTF-8 字节(图片、protobuf、agent 未能解开的压缩流等),
    字段里是标准 base64。
  缺 `body_encoding` 按 `utf8` 解释;空 body 用 `""` 或 `null`。发送端必须把标签写对。
- **压缩由 agent 透明解开。** agent 会剥掉调用方带来的 `accept-encoding`,自行向上游
  协商 gzip / brotli 并解压。因此回到调用方的 body 已是**解压后的明文**,响应 header
  里也不会再出现 `content-encoding` / `content-length`。文本响应几乎总是落在 `utf8`。
  (上游若用 agent 不支持的编码,则原样透传、`content-encoding` 保留,body 多半是 `base64`。)

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
| `UpstreamUnreachable` | `detail: string` | B 连不上目标服务 |
| `UpstreamNotAllowed` | `url: string` | 目标地址不在 agent 的允许清单内(安全边界,非故障) |
| `UpstreamTimeout` | — | 本地服务在 deadline 内没返回 |
| `Expired` | — | 拉取到时已超过 deadline |
| `TooLarge` | `limit: number` | body 超出大小限制 |
| `Internal` | `detail: string` | 其他内部错误 |

## 限制

- `body` 上限 4 MiB,校验的是**编码之前的原始字节数**(base64 / utf8 都一样,响应按
  解压后的字节算)。发送前和回写前都要检查。
- 以下 hop-by-hop header 在转发前必须剔除,透传会导致 HTTP 客户端行为异常:
  `host`、`connection`、`content-length`、`transfer-encoding`、`upgrade`、
  `keep-alive`、`proxy-authenticate`、`proxy-authorization`、`te`、`trailer`。
- header value 用 string 表示。HTTP 规范允许任意字节,实践中都是 ASCII;
  非 UTF-8 的值会被 lossy 转换。
- 只支持一问一答,表达不了 SSE / WebSocket / chunked streaming。

## Go 参考结构

**body 不能直接用 `[]byte`**(那会让 `encoding/json` 一律 base64,和本协议「文本存原文」
的规则冲突)。请求和响应都用 `body_encoding` + `string`,发送时按内容挑编码、接收时按标签
解读。下面的 `Body`/`bytesFromBody` 两个 helper 请求响应通用。

```go
// 出向:按内容挑编码。能当合法 UTF-8 就存原文,否则 base64。
func encodeBody(b []byte) (encoding, body string) {
	if utf8.Valid(b) {
		return "utf8", string(b)
	}
	return "base64", base64.StdEncoding.EncodeToString(b)
}

// 入向:base64 才解码,utf8 / 缺省就是原文。
func bytesFromBody(encoding, body string) ([]byte, error) {
	if encoding == "base64" {
		return base64.StdEncoding.DecodeString(body)
	}
	return []byte(body), nil
}

type Request struct {
	ReqID        string      `json:"req_id"`
	ReplyTo      string      `json:"reply_to"`      // 仅 queue 模式用;kv 模式填 ""
	ReplyMode    string      `json:"reply_mode"`    // "queue"(默认) | "kv"
	Method       string      `json:"method"`
	URL          string      `json:"url"`
	Headers      [][2]string `json:"headers"`
	BodyEncoding string      `json:"body_encoding"` // "utf8" | "base64",缺省按 utf8
	Body         string      `json:"body"`          // 原文或 base64,按上一字段解读
	Deadline     int64       `json:"deadline"`
}

type Response struct {
	ReqID  string `json:"req_id"`
	Result struct {
		Ok  *Ok          `json:"Ok,omitempty"`
		Err *BridgeError `json:"Err,omitempty"`
	} `json:"result"`
}

type Ok struct {
	Status       int         `json:"status"`
	Headers      [][2]string `json:"headers"`
	BodyEncoding string      `json:"body_encoding"` // "utf8" | "base64",缺省按 utf8
	Body         string      `json:"body"`          // 原文或 base64,按上一字段解读
}

type BridgeError struct {
	Type   string `json:"type"`
	Detail string `json:"detail,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	URL    string `json:"url,omitempty"` // UpstreamNotAllowed 时给出被拒的地址
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
