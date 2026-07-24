//! 协议层:消息定义 + 编解码 + Redis key 约定。
//!
//! 线上格式是 JSON:body 走 base64,其余字段保持明文,便于直接用
//! `redis-cli LRANGE` 排查。跨语言实现请对照 `PROTOCOL.md`。

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::prelude::*;
use uuid::Uuid;

/// 单条消息 body 上限。校验的是**原始字节数**,不是 base64 之后的长度,
/// 否则实际限额会漂移 33%。A 侧发送前、B 侧回写前都必须检查。
pub const MAX_BODY_SIZE: usize = 4 * 1024 * 1024; // 4 MB

/// 回复队列(Queue 模式)的 TTL,防止 A 实例崩溃后队列永久残留。
pub const RESP_TTL_SECS: i64 = 60;

/// KV 模式响应的存活时间 —— 调用方「发完先走、之后再来取」的领取窗口。比队列模式的
/// 保险丝更长,因为领取本就是延迟的;超过窗口响应被回收,`GET` 得到 nil。按需调整。
pub const RESP_KV_TTL_SECS: i64 = 600;

// ---------------------------------------------------------------------------
// 消息定义
// ---------------------------------------------------------------------------

/// 响应投递方式,由请求方在 `HttpRequest.reply_mode` 指定。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplyMode {
    /// 队列模式(默认):`LPUSH bridge:resp:{reply_to}` + `EXPIRE`。调用方 `BRPOP`
    /// 阻塞取,响应一到即唤醒。适合同步等待、进程常驻的调用方(如 bridge-client)。
    #[default]
    Queue,
    /// KV 模式:`SET bridge:resp:{req_id} <resp> EX`。调用方之后按 req_id `GET` / `GETDEL`
    /// 自取,发完即可离开、换进程 / 换时间再来。代价是没有阻塞唤醒,要主动来取。
    Kv,
}

/// 请求信封:A 侧 LPUSH 进 `bridge:req:{service}` 的内容。
///
/// body 的线上表示与响应同规则:合法 UTF-8 直接存原文,二进制走 base64,由同级的
/// `body_encoding` 字段标注(手写 serde 见下)。对外 API 仍是干净的 `body: Bytes`。
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub req_id: Uuid,
    /// Queue 模式下的回复队列名(A 实例的 instance_id,每实例一个而非每请求一个)。
    /// KV 模式忽略此字段 —— 响应按 `req_id` 寻址。
    pub reply_to: String,
    /// 响应投递方式。缺字段默认 `Queue`,兼容不认识该字段的旧调用方。
    pub reply_mode: ReplyMode,
    /// GET / POST / ...
    pub method: String,
    /// 逻辑地址:`https://{服务名}/path?query`。host 段是**服务名**而非真实主机,
    /// scheme 只是占位、会被忽略。真实 scheme + host + port 由 agent 的 `upstreams`
    /// 配置决定,所以 Redis 里只出现服务名,真实上游地址不外泄。服务名不在映射内
    /// 一律拒绝(`BridgeError::UnknownUpstream`),决定权始终在 B 侧。
    pub url: String,
    /// header value 用 String 而非字节:HTTP 规范允许任意字节,但实践中都是
    /// ASCII,用 String 才能让 JSON 保持可读。非 UTF-8 的值会被 lossy 转换。
    pub headers: Vec<(String, String)>,
    /// 文本(JSON 等)在线上是原文,二进制是 base64,由 `body_encoding` 标注。
    pub body: Bytes,
    /// Unix 毫秒,绝对时间。agent 拉取到已过期的请求直接回 `Expired`。
    pub deadline: u64,
}

impl Serialize for HttpRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let (encoding, body) = encode_body(&self.body);
        let mut st = serializer.serialize_struct("HttpRequest", 9)?;
        st.serialize_field("req_id", &self.req_id)?;
        st.serialize_field("reply_to", &self.reply_to)?;
        st.serialize_field("reply_mode", &self.reply_mode)?;
        st.serialize_field("method", &self.method)?;
        st.serialize_field("url", &self.url)?;
        st.serialize_field("headers", &self.headers)?;
        st.serialize_field("body_encoding", &encoding)?;
        st.serialize_field("body", &body)?;
        st.serialize_field("deadline", &self.deadline)?;
        st.end()
    }
}

impl<'de> Deserialize<'de> for HttpRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Shadow {
            req_id: Uuid,
            reply_to: String,
            #[serde(default)]
            reply_mode: ReplyMode,
            method: String,
            url: String,
            headers: Vec<(String, String)>,
            #[serde(default)]
            body_encoding: BodyEncoding,
            #[serde(default)]
            body: Option<String>,
            deadline: u64,
        }
        let shadow = Shadow::deserialize(deserializer)?;
        let body = decode_body(shadow.body_encoding, shadow.body.as_deref())
            .map_err(serde::de::Error::custom)?;
        Ok(HttpRequest {
            req_id: shadow.req_id,
            reply_to: shadow.reply_to,
            reply_mode: shadow.reply_mode,
            method: shadow.method,
            url: shadow.url,
            headers: shadow.headers,
            body,
            deadline: shadow.deadline,
        })
    }
}

/// 响应信封 —— Result 语义:B 侧任何失败都必须显式回写错误,
/// 否则 A 只能干等超时,且无法区分「没收到」和「处理失败」。
///
/// serde 把 `Result` 编成外部标签,线上形如 `{"Ok":{...}}` / `{"Err":{...}}`。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpResponse {
    pub req_id: Uuid,
    pub result: Result<HttpOk, BridgeError>,
}

/// body 的线上编码:合法 UTF-8(JSON / 文本)直接以原文上线,`redis-cli` 可直读、
/// 可直接落盘;含非 UTF-8 字节的二进制(图片、gzip 流、protobuf 等)回退到 base64。
/// 用同级的 `body_encoding` 字段区分,解码端据此选择。
///
/// 压缩由 agent 侧的 reqwest 透明解开(见 agent 的 gzip/brotli 特性),所以到这里的
/// body 已是**解压后**的原始内容 —— 文本响应因此几乎总能走 utf8 分支。
#[derive(Debug, Clone)]
pub struct HttpOk {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

/// 请求 / 响应 body 的线上编码标签。缺省(缺字段)按 `utf8` 解释;发送端必须显式写对。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BodyEncoding {
    /// body 是合法 UTF-8,字段里就是原文。
    #[default]
    Utf8,
    /// body 含非 UTF-8 字节,字段里是标准 base64(带填充)。
    Base64,
}

/// 按内容挑编码:能当 UTF-8 解读就存原文,否则 base64。
fn encode_body(body: &Bytes) -> (BodyEncoding, String) {
    match std::str::from_utf8(body) {
        Ok(text) => (BodyEncoding::Utf8, text.to_owned()),
        Err(_) => (BodyEncoding::Base64, STANDARD.encode(body)),
    }
}

/// 反向:utf8 直接取字节,base64 解码。缺字段 / null(Go 的 nil []byte)按空 body 处理。
fn decode_body(encoding: BodyEncoding, raw: Option<&str>) -> Result<Bytes, base64::DecodeError> {
    let raw = raw.unwrap_or_default();
    match encoding {
        BodyEncoding::Utf8 => Ok(Bytes::copy_from_slice(raw.as_bytes())),
        BodyEncoding::Base64 => STANDARD.decode(raw).map(Bytes::from),
    }
}

// 手写 Serialize/Deserialize:对外保持 `body: Bytes` 的清爽 API,线上则拆成
// `body_encoding` + `body` 两个同级字段。单字段 `#[serde(with=..)]` 做不到「一个字段
// 生成两个同级字段」,所以只能在这一层手动展开;其余字段仍交给影子结构体 derive。
impl Serialize for HttpOk {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let (encoding, body) = encode_body(&self.body);
        let mut st = serializer.serialize_struct("HttpOk", 4)?;
        st.serialize_field("status", &self.status)?;
        st.serialize_field("headers", &self.headers)?;
        st.serialize_field("body_encoding", &encoding)?;
        st.serialize_field("body", &body)?;
        st.end()
    }
}

impl<'de> Deserialize<'de> for HttpOk {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Shadow {
            status: u16,
            headers: Vec<(String, String)>,
            #[serde(default)]
            body_encoding: BodyEncoding,
            #[serde(default)]
            body: Option<String>,
        }
        let shadow = Shadow::deserialize(deserializer)?;
        let body = decode_body(shadow.body_encoding, shadow.body.as_deref())
            .map_err(serde::de::Error::custom)?;
        Ok(HttpOk {
            status: shadow.status,
            headers: shadow.headers,
            body,
        })
    }
}

/// 线上错误类型:必须可序列化,因此变体只携带纯数据,不挂 `source`。
///
/// 用内部标签(`{"type":"TooLarge","limit":...}`),这样每个变体在 JSON 里
/// 都是形状一致的对象。默认的外部标签会让无字段变体退化成裸字符串,
/// Go 侧就得为此写一个自定义 `UnmarshalJSON`。
#[derive(Debug, Clone, Serialize, Deserialize, Snafu)]
#[serde(tag = "type")]
pub enum BridgeError {
    #[snafu(display("upstream unreachable: {detail}"))]
    UpstreamUnreachable { detail: String },
    /// 请求里的服务名不在 agent 配置的 upstreams 映射内。安全边界被触发,不是故障。
    #[snafu(display("unknown upstream service {service:?}"))]
    UnknownUpstream { service: String },
    #[snafu(display("upstream timeout"))]
    UpstreamTimeout,
    #[snafu(display("request expired before being handled"))]
    Expired,
    #[snafu(display("body too large (limit {limit} bytes)"))]
    TooLarge { limit: usize },
    #[snafu(display("internal bridge error: {detail}"))]
    Internal { detail: String },
}

// ---------------------------------------------------------------------------
// 编解码
// ---------------------------------------------------------------------------

#[derive(Debug, Snafu)]
pub enum CodecError {
    #[snafu(display("failed to encode bridge message"))]
    Encode { source: serde_json::Error },
    #[snafu(display("failed to decode bridge message"))]
    Decode { source: serde_json::Error },
}

pub fn encode<T: Serialize>(msg: &T) -> Result<Vec<u8>, CodecError> {
    serde_json::to_vec(msg).context(EncodeSnafu)
}

pub fn decode<T: serde::de::DeserializeOwned>(raw: &[u8]) -> Result<T, CodecError> {
    serde_json::from_slice(raw).context(DecodeSnafu)
}

// ---------------------------------------------------------------------------
// Key 约定
// ---------------------------------------------------------------------------

/// 未配置时的默认 key 前缀。
pub const DEFAULT_KEY_PREFIX: &str = "bridge";

/// Redis key 命名空间。所有 key 都是 `{prefix}:req:...` / `{prefix}:resp:...`,
/// 前缀默认 `bridge`,可在启动时由配置覆盖。
///
/// **A / B 两侧必须用同一个前缀。** 否则请求方 LPUSH 到 `{p1}:req:{service}`,agent 却在
/// `{p2}:req:{service}` 上 BRPOP,两边完全看不到对方 —— 这是前缀配错最典型的症状。
#[derive(Debug, Clone)]
pub struct Keyspace {
    prefix: String,
}

impl Keyspace {
    /// 用指定前缀构造。**空白前缀回退到默认值**(即「未配置才用默认」的语义),
    /// 所以调用方把配置值原样传进来即可,不必自己判空。
    pub fn new(prefix: impl AsRef<str>) -> Self {
        let prefix = prefix.as_ref().trim();
        let prefix = if prefix.is_empty() {
            DEFAULT_KEY_PREFIX
        } else {
            prefix
        };
        Self {
            prefix: prefix.to_owned(),
        }
    }

    /// 当前生效的前缀(可用于启动日志)。
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// 请求队列:所有 B 侧 agent 共同 BRPOP,Redis List 天然负载均衡。
    pub fn request_queue(&self, service: &str) -> String {
        format!("{}:req:{service}", self.prefix)
    }

    /// 回复队列(Queue 模式):每个 A 实例一个(不是每个请求一个,避免 N 并发 = N 条阻塞连接)。
    pub fn reply_queue(&self, instance_id: &str) -> String {
        format!("{}:resp:{instance_id}", self.prefix)
    }

    /// KV 模式的响应 key:按 req_id 寻址,请求方之后 `GET {prefix}:resp:{req_id}` 自取。
    /// 与 `reply_queue` 同前缀但类型不同(string vs list);req_id 唯一,不会撞队列名。
    pub fn response_kv_key(&self, req_id: &Uuid) -> String {
        format!("{}:resp:{req_id}", self.prefix)
    }
}

impl Default for Keyspace {
    fn default() -> Self {
        Self::new(DEFAULT_KEY_PREFIX)
    }
}

// ---------------------------------------------------------------------------
// Header 处理:hop-by-hop header 两侧共用的黑名单
// ---------------------------------------------------------------------------

/// 转发时必须剔除的 header(小写)。`Host` 由 HTTP 客户端按 upstream 重写,
/// `Content-Length` 由其重新计算,其余为 hop-by-hop 语义,透传会导致行为异常。
pub const HOP_BY_HOP_HEADERS: &[&str] = &[
    "host",
    "connection",
    "content-length",
    "transfer-encoding",
    "upgrade",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
];

pub fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP_HEADERS
        .iter()
        .any(|h| name.eq_ignore_ascii_case(h))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request(body: Bytes) -> HttpRequest {
        HttpRequest {
            req_id: Uuid::new_v4(),
            reply_to: "inst-1".into(),
            reply_mode: ReplyMode::Queue,
            method: "POST".into(),
            url: "http://127.0.0.1:8080/api/foo?x=1".into(),
            headers: vec![("content-type".into(), "application/json".into())],
            body,
            deadline: 1_700_000_000_000,
        }
    }

    #[test]
    fn roundtrip_request() {
        let req = sample_request(Bytes::from_static(b"\x00\x01binary\xff"));
        let back: HttpRequest = decode(&encode(&req).unwrap()).unwrap();
        assert_eq!(back.req_id, req.req_id);
        assert_eq!(back.body, req.body);
        assert_eq!(back.headers, req.headers);
        assert_eq!(back.deadline, req.deadline);
    }

    /// 文本请求 body 以原文上线(body_encoding=utf8),不再 base64 —— 手工塞请求时
    /// 直接写 JSON 原文即可,redis 里也直读。
    #[test]
    fn request_text_body_is_raw_on_the_wire() {
        let req = sample_request(Bytes::from_static(br#"{"model":"grok"}"#));
        let v: serde_json::Value = serde_json::from_slice(&encode(&req).unwrap()).unwrap();
        assert_eq!(v["body_encoding"], serde_json::json!("utf8"));
        assert_eq!(v["body"], serde_json::json!(r#"{"model":"grok"}"#));
        assert_eq!(v["headers"][0][1], serde_json::json!("application/json"));
    }

    /// 非 UTF-8 的请求 body 回退到 base64。
    #[test]
    fn request_binary_body_uses_base64() {
        let req = sample_request(Bytes::from_static(&[0x00u8, 0x01, 0xff]));
        let v: serde_json::Value = serde_json::from_slice(&encode(&req).unwrap()).unwrap();
        assert_eq!(v["body_encoding"], serde_json::json!("base64"));
        assert_eq!(v["body"], serde_json::json!("AAH/"));
    }

    /// Go 的 json.Marshal 把 nil []byte 编成 null,解码端必须容忍。
    #[test]
    fn null_body_decodes_as_empty() {
        let raw = br#"{"req_id":"67e55044-10b1-426f-9247-bb680e5fe0c8","reply_to":"i",
            "method":"GET","url":"http://h/","headers":[],"body":null,"deadline":1}"#;
        let req: HttpRequest = decode(raw).unwrap();
        assert!(req.body.is_empty());
    }

    /// 旧报文没有 reply_mode 字段 → 默认 Queue,兼容既有调用方。
    #[test]
    fn reply_mode_defaults_to_queue_when_absent() {
        let raw = br#"{"req_id":"67e55044-10b1-426f-9247-bb680e5fe0c8","reply_to":"i",
            "method":"GET","url":"http://h/","headers":[],"body":"","deadline":1}"#;
        let req: HttpRequest = decode(raw).unwrap();
        assert_eq!(req.reply_mode, ReplyMode::Queue);
    }

    /// KV 模式在线上是 `"reply_mode":"kv"`,且能往返。
    #[test]
    fn reply_mode_kv_roundtrips() {
        let mut req = sample_request(Bytes::from_static(b"x"));
        req.reply_mode = ReplyMode::Kv;
        let v: serde_json::Value = serde_json::from_slice(&encode(&req).unwrap()).unwrap();
        assert_eq!(v["reply_mode"], serde_json::json!("kv"));
        let back: HttpRequest = decode(&encode(&req).unwrap()).unwrap();
        assert_eq!(back.reply_mode, ReplyMode::Kv);
    }

    /// 每个错误变体都应是带 `type` 字段的对象,无字段变体也不例外。
    #[test]
    fn errors_are_uniformly_tagged_objects() {
        let cases = [
            (BridgeError::UpstreamTimeout, serde_json::json!({"type": "UpstreamTimeout"})),
            (
                BridgeError::TooLarge { limit: 42 },
                serde_json::json!({"type": "TooLarge", "limit": 42}),
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(serde_json::to_value(&err).unwrap(), expected);
        }
    }

    #[test]
    fn roundtrip_response_err() {
        let resp = HttpResponse {
            req_id: Uuid::new_v4(),
            result: Err(BridgeError::TooLarge {
                limit: MAX_BODY_SIZE,
            }),
        };
        let back: HttpResponse = decode(&encode(&resp).unwrap()).unwrap();
        assert!(matches!(
            back.result,
            Err(BridgeError::TooLarge { limit }) if limit == MAX_BODY_SIZE
        ));
    }

    fn sample_ok(body: Bytes) -> HttpOk {
        HttpOk {
            status: 200,
            headers: vec![("content-type".into(), "application/json".into())],
            body,
        }
    }

    /// 文本 body 以原文上线(body_encoding=utf8),不再 base64 —— redis 可直读、可落盘。
    #[test]
    fn ok_text_body_is_raw_on_the_wire() {
        let ok = sample_ok(Bytes::from_static("hi 世界".as_bytes()));
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v["body_encoding"], serde_json::json!("utf8"));
        assert_eq!(v["body"], serde_json::json!("hi 世界"));
    }

    /// 非 UTF-8 的 body 回退到 base64,标签相应变为 base64,且能无损还原。
    #[test]
    fn ok_binary_body_falls_back_to_base64() {
        let ok = sample_ok(Bytes::from_static(&[0x00u8, 0x01, 0xff]));
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v["body_encoding"], serde_json::json!("base64"));
        assert_eq!(v["body"], serde_json::json!("AAH/"));
        let back: HttpOk = serde_json::from_value(v).unwrap();
        assert_eq!(back.body, ok.body);
    }

    /// 文本与二进制两条路径都要能编解码往返。
    #[test]
    fn ok_body_roundtrips_both_encodings() {
        for body in [
            Bytes::from_static(b"plain text body"),
            Bytes::from_static(&[0xdeu8, 0xad, 0xbe, 0xef]),
        ] {
            let ok = sample_ok(body.clone());
            let back: HttpOk = decode(&encode(&ok).unwrap()).unwrap();
            assert_eq!(back.body, body);
            assert_eq!(back.status, 200);
        }
    }

    /// 缺 body 字段(Go 的 nil []byte → 省略 / null)按空 body 处理,不报错。
    #[test]
    fn ok_missing_body_decodes_as_empty() {
        let ok: HttpOk = decode(br#"{"status":204,"headers":[]}"#).unwrap();
        assert!(ok.body.is_empty());
        assert_eq!(ok.status, 204);
    }

    #[test]
    fn hop_by_hop_filter() {
        assert!(is_hop_by_hop("Host"));
        assert!(is_hop_by_hop("CONNECTION"));
        assert!(!is_hop_by_hop("content-type"));
    }

    #[test]
    fn key_naming_default_prefix() {
        let ks = Keyspace::default();
        assert_eq!(ks.request_queue("svc"), "bridge:req:svc");
        assert_eq!(ks.reply_queue("abc"), "bridge:resp:abc");
        assert_eq!(
            ks.response_kv_key(&Uuid::nil()),
            "bridge:resp:00000000-0000-0000-0000-000000000000"
        );
    }

    /// 自定义前缀生效;空白前缀回退到默认(未配置才用默认)。
    #[test]
    fn key_prefix_override_and_blank_fallback() {
        assert_eq!(Keyspace::new("ferryx").request_queue("svc"), "ferryx:req:svc");
        assert_eq!(Keyspace::new("  ").request_queue("svc"), "bridge:req:svc");
        assert_eq!(Keyspace::new("").prefix(), "bridge");
    }
}
