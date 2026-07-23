//! ferry 与 `tibba-cache` 的接线层。
//!
//! 这里**不实现**任何连接池、拓扑判定或连接管理 —— 那些都在 `tibba-cache` 里。
//! 本 crate 只做两件 ferry 特有的事:
//! 1. 把 ferry 的 `REDIS_URL` 环境变量翻译成 tibba 的配置结构;
//! 2. 转出 ferry 用得到的那几个 tibba 类型,免得每个调用方都去依赖 tibba-cache。

use snafu::prelude::*;

// 直接转出,调用方按 tibba 的接口用,不必自己去依赖 tibba-cache
pub use tibba_cache::{brpop_timeout_secs, RedisClient, RedisClientConn, RedisDedicatedConn};

/// tibba-cache 在借用连接、执行命令时返回的错误。
pub use tibba_cache::Error as CacheError;

/// 转出 `redis` crate,调用方用它构造 BRPOP / LPUSH 等命令。
///
/// 转的是 **tibba-cache 自己转出的那个**,而不是 ferry 另行声明的依赖 —— 这样
/// 版本对齐由 tibba 单方面保证。两边各自声明的话,一旦版本分叉,
/// `ConnectionLike` 就不是同一个 trait,连接根本传不进去。
pub use tibba_cache::redis;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("redis uri must not be empty"))]
    EmptyUri,
    #[snafu(display("failed to build the redis config"))]
    Config { source: tibba_config::Error },
    #[snafu(display("failed to create the redis client"))]
    Client { source: tibba_cache::Error },
}

/// 环境变量前缀。除 `REDIS_URL` 外,还可以用 `FERRY__REDIS__POOL_SIZE` 这类
/// 变量覆盖 tibba 的其余参数(层级分隔符是双下划线)。
const ENV_PREFIX: &str = "FERRY";

/// 由一个 Redis URI 构造 `tibba_cache::RedisClient`。
///
/// URI 直接交给 tibba 解析,因此支持它的聚合写法 —— 一个 scheme + 逗号分隔的
/// 多个 host + 查询参数,例如:
///
/// ```text
/// redis://:password@10.0.0.1:6379,10.0.0.2:6379/?pool_size=20&connection_timeout=3s
/// ```
///
/// 密码写一次即可,不需要在每个节点上重复;`rediss://` 也会被保留为 TLS。
pub fn connect(uri: &str) -> Result<RedisClient, Error> {
    ensure!(!uri.trim().is_empty(), EmptyUriSnafu);

    // tibba-config 以内联 TOML 为基底、环境变量作为覆盖层。把 URI 塞进 TOML,
    // 就能既保留 ferry 原有的 REDIS_URL,又允许 FERRY__REDIS__* 覆盖细项。
    // 用 Debug 格式做转义,密码里的特殊字符不会破坏 TOML。
    let inline = format!("[redis]\nuri = {uri:?}\n");
    let config = tibba_config::Config::builder()
        .add_toml(inline)
        .with_env_prefix(ENV_PREFIX)
        .build()
        .context(ConfigSnafu)?;

    tibba_cache::new_redis_client(&config.sub_config("redis")).context(ClientSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_uri_is_rejected() {
        assert!(connect("").is_err());
        assert!(connect("   ").is_err());
    }

    /// 密码里带引号或反斜杠时,拼 TOML 不能被破坏 —— 所以用 Debug 转义。
    #[test]
    fn uri_is_escaped_into_toml() {
        let uri = r#"redis://:pa"ss\word@127.0.0.1:6379"#;
        let inline = format!("[redis]\nuri = {uri:?}\n");
        let restored = unescape_uri_line(&inline);
        assert_eq!(restored, uri, "转义后应能还原出原始 URI");
    }

    /// 解析 `uri = "..."` 这一行并还原 Rust Debug 字符串字面量的转义。
    /// 只在测试里用,避免为此引入运行时 TOML 依赖。
    fn unescape_uri_line(s: &str) -> String {
        let raw = s
            .lines()
            .find_map(|l| l.strip_prefix("uri = "))
            .expect("missing uri line")
            .trim();
        let inner = raw
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .expect("not a quoted string");
        let mut out = String::new();
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                out.push(chars.next().expect("dangling escape"));
            } else {
                out.push(c);
            }
        }
        out
    }
}
