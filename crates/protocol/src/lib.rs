//! 协议层:消息定义 + 编解码 + Redis key 约定。
//!
//! 线上格式是 JSON:body 走 base64,其余字段保持明文,便于直接用
//! `redis-cli LRANGE` 排查。跨语言实现请对照 `PROTOCOL.md`。

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use uuid::Uuid;

/// 单条消息 body 上限。校验的是**原始字节数**,不是 base64 之后的长度,
/// 否则实际限额会漂移 33%。A 侧发送前、B 侧回写前都必须检查。
pub const MAX_BODY_SIZE: usize = 4 * 1024 * 1024; // 4 MB

/// 回复队列的 TTL,防止 A 实例崩溃后队列永久残留。
pub const RESP_TTL_SECS: i64 = 60;

// ---------------------------------------------------------------------------
// 消息定义
// ---------------------------------------------------------------------------

/// 请求信封:A 侧 LPUSH 进 `bridge:req:{service}` 的内容。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRequest {
    pub req_id: Uuid,
    /// A 实例的 instance_id,agent 据此定位回复队列(每实例一个,非每请求一个)。
    pub reply_to: String,
    /// GET / POST / ...
    pub method: String,
    /// /api/foo?x=1
    pub uri: String,
    /// header value 用 String 而非字节:HTTP 规范允许任意字节,但实践中都是
    /// ASCII,用 String 才能让 JSON 保持可读。非 UTF-8 的值会被 lossy 转换。
    pub headers: Vec<(String, String)>,
    /// 线上是 base64 字符串,对应 Go 的 `[]byte`。
    #[serde(with = "b64")]
    pub body: Bytes,
    /// Unix 毫秒,绝对时间。agent 拉取到已过期的请求直接回 `Expired`。
    pub deadline: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpOk {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    #[serde(with = "b64")]
    pub body: Bytes,
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
// base64 编解码:与 Go `encoding/json` 处理 `[]byte` 的方式一致
// ---------------------------------------------------------------------------

mod b64 {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&STANDARD.encode(value))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        // Go 的 `json.Marshal` 会把 nil []byte 编成 null,这里一并当空 body 处理
        let encoded = Option::<String>::deserialize(de)?.unwrap_or_default();
        STANDARD
            .decode(encoded.as_bytes())
            .map(Bytes::from)
            .map_err(serde::de::Error::custom)
    }
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

/// 请求队列:所有 B 侧 agent 共同 BRPOP,Redis List 天然负载均衡。
pub fn request_queue(service: &str) -> String {
    format!("bridge:req:{service}")
}

/// 回复队列:每个 A 实例一个(不是每个请求一个,避免 N 并发 = N 条阻塞连接)。
pub fn reply_queue(instance_id: &str) -> String {
    format!("bridge:resp:{instance_id}")
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
            method: "POST".into(),
            uri: "/api/foo?x=1".into(),
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

    /// body 必须是 base64 字符串,而不是 serde 默认的数字数组 —— 后者会膨胀
    /// 三倍,也无法对应 Go 的 `[]byte`。
    #[test]
    fn body_is_base64_string_on_the_wire() {
        let req = sample_request(Bytes::from_static(b"hello"));
        let v: serde_json::Value = serde_json::from_slice(&encode(&req).unwrap()).unwrap();
        assert_eq!(v["body"], serde_json::json!("aGVsbG8="));
        assert_eq!(v["headers"][0][1], serde_json::json!("application/json"));
    }

    /// Go 的 json.Marshal 把 nil []byte 编成 null,解码端必须容忍。
    #[test]
    fn null_body_decodes_as_empty() {
        let raw = br#"{"req_id":"67e55044-10b1-426f-9247-bb680e5fe0c8","reply_to":"i",
            "method":"GET","uri":"/","headers":[],"body":null,"deadline":1}"#;
        let req: HttpRequest = decode(raw).unwrap();
        assert!(req.body.is_empty());
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

    #[test]
    fn hop_by_hop_filter() {
        assert!(is_hop_by_hop("Host"));
        assert!(is_hop_by_hop("CONNECTION"));
        assert!(!is_hop_by_hop("content-type"));
    }

    #[test]
    fn key_naming() {
        assert_eq!(request_queue("svc"), "bridge:req:svc");
        assert_eq!(reply_queue("abc"), "bridge:resp:abc");
    }
}
