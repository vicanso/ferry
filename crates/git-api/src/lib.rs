//! REST 服务:按 `repo` / `branch` / `prevCommit` / `currentCommit`
//! 查两个 commit 之间的完整 patch。
//!
//! ```text
//! GET /patch?repo=ferry&branch=main&prevCommit=abc1234&currentCommit=def5678
//! GET /health
//! ```
//!
//! # 为什么只 fetch,不 checkout
//!
//! `git diff A B` 是纯对象库操作,**不需要工作区**。checkout + pull 唯一的作用是
//! 把缺失的对象拉到本地,而 `fetch` 就够了。只 fetch 换来三件事:
//!
//! - 不动工作区 ⇒ 并发请求同一个仓库的不同分支不会互相踩;
//! - 目录被人手工改脏、或本地分叉,都不再让请求失败;
//! - 根目录下可以放 bare mirror(`git clone --mirror`),省掉一份检出文件。
//!
//! 两个 commit 本地都已存在时连 fetch 都跳过:给定 sha 后 diff 结果是确定的,
//! 拉不拉都一样,省一次网络往返。
//!
//! # 首次使用时自动 clone
//!
//! 仓库不在根目录里时,按配置的 `[server.repos]` 白名单(name → URL)克隆一份 bare
//! 仓库。**只认白名单**:请求给的是名字,URL 完全由配置决定,调用方碰不到 —— 与
//! agent 的 upstreams 是同一条边界。名字不在表里就 404,绝不按请求里的东西去连
//! 外部地址。
//!
//! # 安全边界
//!
//! `repo` 来自请求,拼进路径就是任意目录读取,所以 [`resolve_repo`] 做两道拦截:
//! 先按语法拒绝(空段、`.`、`..`、反斜杠、NUL),再 `canonicalize` 后确认仍在根
//! 目录内 —— 后者才挡得住根目录里指向外部的符号链接,光看字符串是看不出来的。
//!
//! `prevCommit` / `currentCommit` 直接交给 libgit2 的 revparse。这里没有命令注入
//! 的面:全程调库,不 fork shell;能读到的对象也都在该仓库自己的边界内。
//!
//! # 库 / 二进制
//!
//! 本 crate 既是库也是二进制。库不装日志、不监听信号 —— 那两样全局只能做一次。
//! 同进程里还跑着 bridge-agent 时(`ferry` 二进制),两者共用一个 subscriber 和
//! 同一个 `CancellationToken`;此时本服务只绑回环,入口收敛到 Redis。

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use dashmap::DashMap;
use ferry_git::{DiffBase, Repo};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

/// 烘焙进二进制的默认配置
const DEFAULT_CONFIG: &str = include_str!("../config/default.toml");
const CONFIG_PATH_ENV: &str = "FERRY_CONFIG";
const ENV_PREFIX: &str = "FERRY";

#[derive(Debug, Snafu)]
pub enum StartupError {
    #[snafu(display("failed to read the configuration file {path}"))]
    ReadConfig {
        path: String,
        source: std::io::Error,
    },
    #[snafu(display("failed to load the configuration"))]
    Config { source: tibba_config::Error },
    #[snafu(display("server.root must not be empty; set FERRY__SERVER__ROOT"))]
    EmptyRoot,
    #[snafu(display("server.root {path} is not an accessible directory"))]
    BadRoot {
        path: String,
        source: std::io::Error,
    },
    #[snafu(display("server.repos entry {name:?} has an empty url"))]
    EmptyRepoUrl { name: String },
    #[snafu(display("server.max_concurrency must be > 0"))]
    ZeroConcurrency,
    #[snafu(display("invalid listen address {addr}"))]
    BadAddr {
        addr: String,
        source: std::net::AddrParseError,
    },
    #[snafu(display("failed to bind {addr}"))]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[snafu(display("server failed"))]
    Serve { source: std::io::Error },
}

#[derive(Debug, Deserialize)]
struct RawServer {
    addr: String,
    root: String,
    remote: String,
    max_concurrency: usize,
    max_patch_bytes: usize,
    /// 允许自动 clone 的仓库:名字 → URL。缺省为空表(即不自动 clone)。
    #[serde(default)]
    repos: HashMap<String, String>,
}

struct AppState {
    /// 已 canonicalize 的根目录。比对必须用规范化后的,否则符号链接能绕过。
    root: PathBuf,
    /// 白名单:仓库名 → clone URL。不在表里的仓库不会被自动克隆。
    repos: HashMap<String, String>,
    remote: String,
    max_patch_bytes: usize,
    limit: Arc<Semaphore>,
    /// 每个仓库一把锁。fetch 会写 refs,同一仓库并发 fetch 可能撞上 git 的引用锁,
    /// 串行化掉这类偶发错误;不同仓库之间互不影响。
    locks: DashMap<PathBuf, Arc<Mutex<()>>>,
}

impl AppState {
    fn lock_for(&self, path: &Path) -> Arc<Mutex<()>> {
        self.locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchQuery {
    repo: String,
    branch: String,
    prev_commit: String,
    current_commit: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PatchResponse {
    repo: String,
    branch: String,
    /// 解析后的完整 sha(请求里给短 sha 也会补全),便于调用方留档
    prev_commit: String,
    current_commit: String,
    /// 是否为了这次请求真的走了网络
    fetched: bool,
    /// 本次是否刚按白名单克隆了这个仓库
    cloned: bool,
    files: Vec<String>,
    insertions: usize,
    deletions: usize,
    patch: String,
}

type ApiError = tibba_error::Error;

fn err(status: u16, sub: &str, msg: impl ToString) -> ApiError {
    ApiError::new(msg)
        .with_category("git_patch")
        .with_sub_category(sub)
        .with_status(status)
}

/// 把请求里的 `repo` 解析成根目录下的路径,拒绝一切逃逸。
///
/// 允许多级(`group/project`),因为 GitLab 风格的分组很常见;但每一段都要过筛。
///
/// 目标**可以尚不存在**(留给自动 clone),所以不能拿整条路径去 canonicalize —— 对不
/// 存在的路径它必然失败。改为逐级向上找到第一个真实存在的祖先再规范化比对:这样即便
/// 路径中间有指向根目录外的符号链接,也在 open / clone 动手之前就被挡下。
fn resolve_repo(root: &Path, repo: &str) -> Result<PathBuf, ApiError> {
    let reject = |why: &str| err(400, "invalid_repo", format!("invalid repo {repo:?}: {why}"));

    if repo.is_empty() {
        return Err(reject("empty"));
    }
    for seg in repo.split('/') {
        // 空段同时覆盖了绝对路径(前导 /)和连续斜杠
        if seg.is_empty() {
            return Err(reject("empty path segment"));
        }
        if seg == "." || seg == ".." {
            return Err(reject("path traversal"));
        }
        if seg.contains('\\') || seg.contains('\0') {
            return Err(reject("illegal character"));
        }
    }

    let candidate = root.join(repo);
    // 语法检查挡不住「根目录里有个软链指向 /etc」,只有规范化后比对真实路径才行。
    let mut probe = candidate.as_path();
    let anchor = loop {
        if let Ok(real) = probe.canonicalize() {
            break real;
        }
        match probe.parent() {
            Some(parent) => probe = parent,
            None => return Err(reject("escapes the configured root")),
        }
    };
    if !anchor.starts_with(root) {
        // 不把真实路径回显给调用方 —— 那等于帮探测者确认目标存在于哪里
        return Err(reject("escapes the configured root"));
    }
    Ok(candidate)
}

/// 把 ferry-git 的错误翻成合适的 HTTP 状态。
///
/// 分清「调用方给错了」(4xx)和「服务端 / 远端出问题」(5xx):前者重试无意义,
/// 后者值得重试或告警。
fn map_git_err(e: ferry_git::Error) -> ApiError {
    use snafu::Report;
    use ferry_git::Error as G;
    match e {
        G::Open { .. } => err(404, "not_a_repo", "not a git repository"),
        // clone 失败多半是远端不可达或凭证不对,属服务端侧
        G::Clone { .. } => err(502, "clone_failed", Report::from_error(e).to_string()),
        G::BadRevision { ref rev, .. } => err(
            404,
            "unknown_revision",
            format!("revision {rev:?} not found (fetched already; wrong sha or wrong branch?)"),
        ),
        G::RemoteNotFound { ref remote, .. } => {
            err(500, "remote_not_found", format!("remote {remote:?} not configured"))
        }
        // 远端不可达 / 认证失败:服务端侧问题,502 更诚实
        G::Fetch { .. } => err(502, "fetch_failed", Report::from_error(e).to_string()),
        other => err(500, "git_error", Report::from_error(other).to_string()),
    }
}

async fn health() -> &'static str {
    "ok"
}

async fn patch(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PatchQuery>,
) -> Result<Json<PatchResponse>, ApiError> {
    let path = resolve_repo(&state.root, &q.repo)?;

    // 先限流再抢仓库锁:反过来的话,等锁的请求会一直占着信号量名额
    let _permit = state
        .limit
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| err(503, "shutting_down", "server is shutting down"))?;
    let lock = state.lock_for(&path);
    let _guard = lock.lock().await;

    // 存在性判断必须在拿到仓库锁之后:否则两个并发请求会同时判定「不存在」,
    // 后一个往前一个刚克到一半的目录里再克隆,libgit2 直接报「目录非空」。
    let clone_url = if path.exists() {
        None
    } else {
        match state.repos.get(&q.repo) {
            Some(url) => {
                tracing::info!(repo = %q.repo, "repo not present; cloning on first use");
                Some(url.clone())
            }
            // 不在白名单就到此为止 —— 绝不拿请求里的东西去拼 URL 连外部地址
            None => {
                return Err(err(
                    404,
                    "repo_not_found",
                    format!(
                        "repo {:?} is not cloned under server.root and not listed in server.repos",
                        q.repo
                    ),
                ))
            }
        }
    };

    let (remote, branch) = (state.remote.clone(), q.branch.clone());
    let (prev, cur) = (q.prev_commit.clone(), q.current_commit.clone());
    let work_path = path.clone();

    // git2 全是阻塞调用,clone / fetch 还要走网络,绝不能占着 async 执行器
    let outcome = tokio::task::spawn_blocking(move || {
        let cloned = clone_url.is_some();
        let repo = match clone_url {
            Some(url) => Repo::clone_bare(&url, &work_path)?,
            None => Repo::open(&work_path)?,
        };

        // 两个对象本地都有就不必联网:给定 sha,diff 结果与是否 fetch 无关
        // 刚克隆的仓库对象是齐的,不必再联网;否则缺哪个 sha 就拉一次该分支
        let have = |rev: &str| repo.raw().revparse_single(rev).is_ok();
        let fetched = if have(&prev) && have(&cur) {
            false
        } else {
            repo.fetch(&remote, &branch)?;
            true
        };

        // 回显完整 sha:调用方常给短 sha,补全后便于留档与复查
        let full = |rev: &str| -> Result<String, ferry_git::Error> {
            Ok(repo
                .raw()
                .revparse_single(rev)
                .map_err(|source| ferry_git::Error::BadRevision {
                    rev: rev.to_string(),
                    source,
                })?
                .id()
                .to_string())
        };
        let (prev_full, cur_full) = (full(&prev)?, full(&cur)?);

        let report = repo.diff(&prev, &cur, DiffBase::Direct)?;
        Ok::<_, ferry_git::Error>((report, prev_full, cur_full, fetched, cloned))
    })
    .await
    .map_err(|e| err(500, "join_error", format!("git task panicked: {e}")))?
    .map_err(map_git_err)?;

    let (report, prev_commit, current_commit, fetched, cloned) = outcome;

    if report.patch.len() > state.max_patch_bytes {
        // 截断过的 patch 既 apply 不了也没法信,不如明确失败并告知实际大小
        return Err(err(
            413,
            "patch_too_large",
            format!(
                "patch is {} bytes, over the {} byte limit",
                report.patch.len(),
                state.max_patch_bytes
            ),
        ));
    }

    tracing::info!(
        repo = %q.repo,
        branch = %q.branch,
        prev = %prev_commit,
        current = %current_commit,
        fetched,
        cloned,
        files = report.files.len(),
        bytes = report.patch.len(),
        "patch served"
    );

    Ok(Json(PatchResponse {
        repo: q.repo,
        branch: q.branch,
        prev_commit,
        current_commit,
        fetched,
        cloned,
        files: report.files,
        insertions: report.insertions,
        deletions: report.deletions,
        patch: report.patch,
    }))
}

/// 校验过的运行参数。字段私有:入口只负责把它从 [`load_config`] 传给 [`serve`],
/// 不该也不需要在外面拆开改。
pub struct ApiConfig {
    addr: SocketAddr,
    /// 已 canonicalize,请求路径的前缀比对全靠它
    root: PathBuf,
    repos: HashMap<String, String>,
    remote: String,
    max_concurrency: usize,
    max_patch_bytes: usize,
}

/// 读取并校验配置。
///
/// `server.root` 为空时返回 [`StartupError::EmptyRoot`] —— 入口据此把「没配这个
/// 子系统」与「配了但配错了」区分开。
pub fn load_config() -> Result<ApiConfig, StartupError> {
    let mut builder = tibba_config::Config::builder().add_toml(DEFAULT_CONFIG);
    if let Ok(path) = std::env::var(CONFIG_PATH_ENV) {
        let data = std::fs::read_to_string(&path).context(ReadConfigSnafu { path })?;
        builder = builder.add_toml(data);
    }
    let config = builder
        .with_env_prefix(ENV_PREFIX)
        .build()
        .context(ConfigSnafu)?;
    // 按段取而不是整体反序列化:与 agent 同因 —— prefix 为空时 config-rs 会报
    // "invalid identifier"
    let server: RawServer = config
        .sub_config("server")
        .try_deserialize()
        .context(ConfigSnafu)?;

    ensure!(!server.root.trim().is_empty(), EmptyRootSnafu);
    ensure!(server.max_concurrency > 0, ZeroConcurrencySnafu);
    // 空 URL 是最容易犯的配置错(变量拼错就成了空串),启动即失败好过每次请求 502
    for (name, url) in &server.repos {
        ensure!(!url.trim().is_empty(), EmptyRepoUrlSnafu { name });
    }

    // 启动就 canonicalize 一次:之后每个请求都拿它做前缀比对,不能每次再解析
    let root = PathBuf::from(server.root.trim())
        .canonicalize()
        .context(BadRootSnafu {
            path: server.root.clone(),
        })?;

    let addr: SocketAddr = server.addr.parse().context(BadAddrSnafu {
        addr: server.addr.clone(),
    })?;
    Ok(ApiConfig {
        addr,
        root,
        repos: server.repos,
        remote: server.remote,
        max_concurrency: server.max_concurrency,
        max_patch_bytes: server.max_patch_bytes,
    })
}

/// 启动 HTTP 服务,直到 `shutdown` 被取消后优雅退出(停止收新连接,等在途请求做完)。
pub async fn serve(cfg: ApiConfig, shutdown: CancellationToken) -> Result<(), StartupError> {
    let ApiConfig {
        addr,
        root,
        repos,
        remote,
        max_concurrency,
        max_patch_bytes,
    } = cfg;

    // 只列名字,不列 URL:URL 里可能带 token(https://user:token@host/...)
    let repo_names = repos.keys().cloned().collect::<Vec<_>>().join(", ");

    let state = Arc::new(AppState {
        root: root.clone(),
        repos,
        remote: remote.clone(),
        max_patch_bytes,
        limit: Arc::new(Semaphore::new(max_concurrency)),
        locks: DashMap::new(),
    });

    let app = Router::new()
        .route("/patch", get(patch))
        .route("/health", get(health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(BindSnafu { addr })?;
    tracing::info!(
        %addr,
        root = %root.display(),
        %remote,
        max_concurrency,
        auto_clone = %if repo_names.is_empty() { "(none)".to_string() } else { repo_names },
        "git patch api listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .context(ServeSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 路径逃逸是本服务最要命的一类问题,逐种写法都得挡住。
    #[test]
    fn resolve_repo_rejects_escapes() {
        let root_dir = tempfile::tempdir().expect("tempdir");
        let root = root_dir.path().canonicalize().expect("canonicalize");
        std::fs::create_dir(root.join("ok")).expect("mkdir");

        // 正常的能过
        assert!(resolve_repo(&root, "ok").is_ok());

        for bad in [
            "",
            "..",
            "../..",
            "ok/../..",
            "/etc",
            "/",
            "ok//x",
            "./ok",
            "..\\ok",
        ] {
            assert!(
                resolve_repo(&root, bad).is_err(),
                "应当拒绝但通过了: {bad:?}"
            );
        }
    }

    /// 语法上人畜无害、但符号链接指向根目录之外 —— 只有 canonicalize 后比对才拦得住。
    #[test]
    fn resolve_repo_rejects_symlink_escape() {
        let root_dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("tempdir");
        let root = root_dir.path().canonicalize().expect("canonicalize");

        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), root.join("sneaky")).expect("symlink");

        assert!(
            resolve_repo(&root, "sneaky").is_err(),
            "指向根目录外的符号链接必须被拒"
        );
    }

    /// 目标不存在不再是错误 —— 它可能是自动 clone 的落点,但路径仍须在根目录内。
    /// (「不在白名单里的仓库要 404」那条判断在 handler 里,不在这一层。)
    #[test]
    fn resolve_repo_allows_missing_target() {
        let root_dir = tempfile::tempdir().expect("tempdir");
        let root = root_dir.path().canonicalize().expect("canonicalize");
        let path = resolve_repo(&root, "not-cloned-yet").expect("目标不存在应当放行");
        assert_eq!(path, root.join("not-cloned-yet"));
        assert!(!path.exists());
    }

    /// 最终目标还不存在、但父目录是指向外部的符号链接 —— 必须拒绝,否则会把仓库
    /// clone 到根目录外面去。这正是「逐级向上找存在的祖先再比对」要挡的情况。
    #[test]
    fn resolve_repo_rejects_symlinked_parent_of_missing_target() {
        let root_dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("tempdir");
        let root = root_dir.path().canonicalize().expect("canonicalize");

        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), root.join("grp")).expect("symlink");

        assert!(
            resolve_repo(&root, "grp/newrepo").is_err(),
            "经由指向外部的软链落地的新仓库必须被拒"
        );
    }
}
