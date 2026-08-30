//! 在**指定目录**的 git 仓库上做三件事:切分支、pull 最新代码、取两个 commit
//! 之间的完整 patch。
//!
//! 通过 libgit2(`git2` crate)直接调用,不 fork `git` 进程。代价是 https / ssh
//! 传输要链 openssl 与 libssh2(libgit2 是 C 库,不认 rustls),详见本 crate 的
//! Cargo.toml。
//!
//! # 安全取向
//!
//! 三个操作里有两个会动工作区,写错就是丢用户**尚未提交**的代码。所以这里一律取
//! 保守语义,宁可报错也不替调用方做取舍:
//!
//! - [`Repo::checkout`] 用 libgit2 的 SAFE 策略,**绝不 force**。和 `git switch`
//!   一样:不冲突的改动可以带着走,会被覆盖的则直接失败(并把脏文件列进错误里)。
//! - [`Repo::pull`] 只做 **fast-forward**。本地与远端分叉时返回
//!   [`PullOutcome::Diverged`] 交给调用方,既不自动 merge 也不 `reset --hard` ——
//!   后者是这类脚本最常见的丢代码方式。
//!
//! # 例子
//!
//! ```no_run
//! use ferry_git::{DiffBase, Repo};
//!
//! let repo = Repo::open("/path/to/repo")?;
//! repo.checkout("main", "origin")?; // 1. 切分支(本地没有就从 origin/main 建)
//! let outcome = repo.pull("origin")?; // 2. pull 最新代码
//! println!("{outcome:?}");
//! // 3. 取两个 commit 之间的完整 patch
//! let report = repo.diff("v1.0.0", "HEAD", DiffBase::Direct)?;
//! println!("{} 个文件, +{} -{}", report.files.len(), report.insertions, report.deletions);
//! print!("{}", report.patch);
//! # Ok::<(), ferry_git::Error>(())
//! ```

use std::path::{Path, PathBuf};

use git2::{
    build::{CheckoutBuilder, RepoBuilder},
    BranchType, Cred, CredentialType, DiffFormat, DiffOptions, FetchOptions, RemoteCallbacks,
    Repository, StatusOptions,
};
use snafu::prelude::*;

/// 转出 `git2::Oid`,调用方不必为了拿一个 commit id 去依赖 git2。
pub use git2::Oid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} is not a git repository", path.display()))]
    Open { path: PathBuf, source: git2::Error },

    #[snafu(display("branch {branch:?} not found locally or on remote {remote:?}"))]
    BranchNotFound { branch: String, remote: String },

    #[snafu(display(
        "worktree has uncommitted changes that would be overwritten: {}",
        files.join(", ")
    ))]
    DirtyWorktree { files: Vec<String> },

    #[snafu(display("HEAD is detached; pull needs a checked-out branch"))]
    DetachedHead,

    #[snafu(display("remote {remote:?} not found"))]
    RemoteNotFound { remote: String, source: git2::Error },

    #[snafu(display("failed to fetch from {remote:?}"))]
    Fetch { remote: String, source: git2::Error },

    #[snafu(display("failed to clone {url:?}"))]
    Clone { url: String, source: git2::Error },

    #[snafu(display("revision {rev:?} cannot be resolved to a commit"))]
    BadRevision { rev: String, source: git2::Error },

    #[snafu(display("git operation failed: {what}"))]
    Git { what: String, source: git2::Error },
}

/// `pull` 的结果。分叉不是错误 —— 它是个需要调用方决策的正常状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullOutcome {
    /// 本地已经是最新,什么都没做。
    UpToDate,
    /// 快进成功,工作区已更新到 `to`。
    FastForward { from: Oid, to: Oid },
    /// 本地有远端没有的提交 —— 需要 merge 或 rebase,本 crate 不替你决定。
    Diverged { local: Oid, remote: Oid },
}

/// 取哪两棵树之间的差异。对应 `git diff` 的两点 / 三点写法。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffBase {
    /// `git diff A B` —— 两个快照的字面差异。
    Direct,
    /// `git diff A...B` —— 从共同祖先(merge base)到 B,即「B 这条线上多出来的
    /// 改动」。code review / PR 看到的就是这个。
    MergeBase,
}

/// [`Repo::diff`] 的结果:patch 原文,外加一份不必二次解析就能用的摘要。
///
/// 摘要直接取自 libgit2 的 `DiffStats` / `deltas`,和 patch 出自同一次 diff,
/// 不存在「自己再 parse 一遍 patch 文本」那种会和原文对不上的风险。
#[derive(Debug, Clone)]
pub struct DiffReport {
    /// `git diff` 原文,可直接 `git apply`。
    pub patch: String,
    /// 涉及的文件路径。改名取新路径,删除取旧路径。
    pub files: Vec<String>,
    pub insertions: usize,
    pub deletions: usize,
}

pub struct Repo {
    inner: Repository,
}

impl Repo {
    /// 打开指定目录的仓库。
    ///
    /// 用 `open` 而非 `discover`:后者会向上逐级找 `.git`,传错路径时会**静默地**
    /// 操作到父仓库上去。自动化脚本里这种「看起来成功了但动错了仓库」远比一句
    /// 报错难查。
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let inner = Repository::open(path).context(OpenSnafu {
            path: path.to_path_buf(),
        })?;
        Ok(Self { inner })
    }

    /// 把 `url` 克隆成一个 **bare** 仓库(无工作区)到 `path`。
    ///
    /// bare 是刻意的:取 patch 只需要对象库,检出一份工作区纯属浪费磁盘和时间。
    /// 认证复用 [`auth_callbacks`],与 [`Repo::fetch`] 完全同一套,不必另配凭证。
    ///
    /// `path` 必须不存在或为空目录 —— libgit2 拒绝往非空目录里克隆,这正好挡住
    /// 「并发两个请求同时克隆同一个仓库」把对方克到一半的目录搅乱。
    pub fn clone_bare(url: &str, path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut opts = FetchOptions::new();
        opts.remote_callbacks(auth_callbacks());
        let inner = RepoBuilder::new()
            .bare(true)
            .fetch_options(opts)
            .clone(url, path.as_ref())
            .context(CloneSnafu { url })?;
        Ok(Self { inner })
    }

    /// 底层 `git2::Repository`,需要本 crate 没覆盖的操作时自取。
    pub fn raw(&self) -> &Repository {
        &self.inner
    }

    /// 1. 切换到指定分支。
    ///
    /// 本地没有该分支时,自动从 `{remote}/{branch}` 建一个跟踪分支(等价于
    /// `git switch -c branch --track origin/branch`),因为 CI / 自动化场景里
    /// 目标分支往往只存在于远端。
    ///
    /// 工作区的未提交改动**不会**被丢弃:用 SAFE 策略,冲突时报
    /// [`Error::DirtyWorktree`] 并列出挡路的文件。
    pub fn checkout(&self, branch: &str, remote: &str) -> Result<(), Error> {
        let refname = self.ensure_local_branch(branch, remote)?;

        let object = self.inner.revparse_single(&refname).context(GitSnafu {
            what: format!("resolve {refname}"),
        })?;

        // 默认就是 SAFE:不覆盖已修改的文件,冲突则整体失败。显式写出来是为了
        // 让后来者一眼看到「这里刻意没有 force」。
        let mut co = CheckoutBuilder::new();
        co.safe();

        if let Err(source) = self.inner.checkout_tree(&object, Some(&mut co)) {
            // libgit2 只会说「N conflicts prevent checkout」,不告诉你是哪些文件。
            // 补上脏文件清单,否则调用方无从下手。
            let files = self.dirty_files().unwrap_or_default();
            if files.is_empty() {
                return Err(source).context(GitSnafu {
                    what: format!("checkout {branch}"),
                });
            }
            return DirtyWorktreeSnafu { files }.fail();
        }

        self.inner.set_head(&refname).context(GitSnafu {
            what: format!("set HEAD to {refname}"),
        })?;
        Ok(())
    }

    /// 从远端拉取 `refspec`(通常就是个分支名),只更新远端跟踪引用,**不动工作区**。
    ///
    /// 认证不自己管:走 ssh-agent / git credential helper,也就是用户 `git fetch`
    /// 本来就在用的那套(见 [`auth_callbacks`])。
    pub fn fetch(&self, remote_name: &str, refspec: &str) -> Result<(), Error> {
        let mut remote = self
            .inner
            .find_remote(remote_name)
            .context(RemoteNotFoundSnafu {
                remote: remote_name,
            })?;
        let mut opts = FetchOptions::new();
        opts.remote_callbacks(auth_callbacks());
        remote
            .fetch(&[refspec], Some(&mut opts), None)
            .context(FetchSnafu {
                remote: remote_name,
            })
    }

    /// 2. 拉取当前分支的最新代码(fetch + fast-forward)。
    ///
    /// **只做快进**。分叉时返回 [`PullOutcome::Diverged`] 而不是自动 merge ——
    /// 自动 merge 会产生无人 review 的合并提交,`reset --hard` 则直接丢掉本地提交。
    pub fn pull(&self, remote_name: &str) -> Result<PullOutcome, Error> {
        let head = self.inner.head().context(GitSnafu { what: "read HEAD" })?;
        ensure!(head.is_branch(), DetachedHeadSnafu);
        // shorthand 返回 Result 而非 Option:它要把引用名当 UTF-8 校验一遍
        let branch = head
            .shorthand()
            .context(GitSnafu {
                what: "read current branch name",
            })?
            .to_string();

        // 只 fetch 当前分支,之后 FETCH_HEAD 就正好指向它的远端最新提交
        self.fetch(remote_name, &branch)?;

        let fetch_head = self.inner.find_reference("FETCH_HEAD").context(GitSnafu {
            what: "read FETCH_HEAD",
        })?;
        let fetched = self
            .inner
            .reference_to_annotated_commit(&fetch_head)
            .context(GitSnafu {
                what: "annotate FETCH_HEAD",
            })?;

        let local = head.target().ok_or(Error::DetachedHead)?;
        let (analysis, _) = self
            .inner
            .merge_analysis(&[&fetched])
            .context(GitSnafu { what: "merge analysis" })?;

        if analysis.is_up_to_date() {
            return Ok(PullOutcome::UpToDate);
        }
        if !analysis.is_fast_forward() {
            return Ok(PullOutcome::Diverged {
                local,
                remote: fetched.id(),
            });
        }

        // 快进:先挪引用,再把工作区对齐到新的 HEAD。仍用 SAFE,本地改动挡路时
        // 宁可失败 —— 此时引用已经动了,但工作区没被破坏,重跑即可。
        let refname = format!("refs/heads/{branch}");
        let mut reference = self.inner.find_reference(&refname).context(GitSnafu {
            what: format!("find {refname}"),
        })?;
        reference
            .set_target(fetched.id(), "pull: fast-forward")
            .context(GitSnafu {
                what: format!("fast-forward {refname}"),
            })?;
        self.inner.set_head(&refname).context(GitSnafu {
            what: format!("set HEAD to {refname}"),
        })?;

        let mut co = CheckoutBuilder::new();
        co.safe();
        self.inner
            .checkout_head(Some(&mut co))
            .context(GitSnafu { what: "checkout after fast-forward" })?;

        Ok(PullOutcome::FastForward {
            from: local,
            to: fetched.id(),
        })
    }

    /// 3. 取两个 commit 之间的完整 patch(`git diff` 原文,可直接 `git apply`)。
    ///
    /// `from` / `to` 是任意 revision 写法:commit sha、`HEAD~3`、tag、分支名都行。
    /// 开了重命名识别,输出与命令行 `git diff` 基本一致。
    pub fn diff(&self, from: &str, to: &str, base: DiffBase) -> Result<DiffReport, Error> {
        let from_commit = self.commit(from)?;
        let to_commit = self.commit(to)?;

        let from_tree = match base {
            DiffBase::Direct => from_commit.tree(),
            DiffBase::MergeBase => {
                let oid = self
                    .inner
                    .merge_base(from_commit.id(), to_commit.id())
                    .context(GitSnafu {
                        what: format!("merge base of {from} and {to}"),
                    })?;
                self.inner
                    .find_commit(oid)
                    .context(GitSnafu { what: "find merge base commit" })?
                    .tree()
            }
        }
        .context(GitSnafu { what: "read base tree" })?;

        let to_tree = to_commit
            .tree()
            .context(GitSnafu { what: "read target tree" })?;

        let mut opts = DiffOptions::new();
        let mut diff = self
            .inner
            .diff_tree_to_tree(Some(&from_tree), Some(&to_tree), Some(&mut opts))
            .context(GitSnafu { what: "diff trees" })?;
        // 命令行 git 默认识别重命名,不开的话一次改名会显示成「整文件删+整文件加」
        diff.find_similar(None)
            .context(GitSnafu { what: "rename detection" })?;

        // 统计和文件列表先取:print 的闭包会可变借用 patch,拿完再渲染更清楚
        let stats = diff.stats().context(GitSnafu { what: "diff stats" })?;
        let files: Vec<String> = diff
            .deltas()
            .filter_map(|d| {
                // 改名后 new_file 才是当前路径;删除时 new_file 无路径,回落到 old_file
                d.new_file()
                    .path()
                    .or_else(|| d.old_file().path())
                    .map(|p| p.display().to_string())
            })
            .collect();

        let mut out = String::new();
        diff.print(DiffFormat::Patch, |_delta, _hunk, line| {
            // 内容行要补回 +/-/空格 前缀;文件头与 hunk 头(origin 为 F/H)自带换行。
            match line.origin() {
                '+' | '-' | ' ' => out.push(line.origin()),
                _ => {}
            }
            out.push_str(&String::from_utf8_lossy(line.content()));
            true
        })
        .context(GitSnafu { what: "render patch" })?;

        Ok(DiffReport {
            patch: out,
            files,
            insertions: stats.insertions(),
            deletions: stats.deletions(),
        })
    }

    /// 工作区里已修改 / 已暂存的文件。未跟踪文件不算 —— 它们不会挡住 checkout。
    fn dirty_files(&self) -> Result<Vec<String>, Error> {
        let mut opts = StatusOptions::new();
        opts.include_untracked(false).include_ignored(false);
        let statuses = self
            .inner
            .statuses(Some(&mut opts))
            .context(GitSnafu { what: "read status" })?;
        // 用 lossy 而不是 path()?:后者遇到非 UTF-8 路径会整体失败,而这里是要给人看
        // 「哪些文件挡住了 checkout」—— 少列一个文件比报个二次错误更误导。
        Ok(statuses
            .iter()
            .map(|e| String::from_utf8_lossy(e.path_bytes()).into_owned())
            .collect())
    }

    /// 确保本地存在该分支,返回它的全名。本地没有就照 `{remote}/{branch}` 建一个
    /// 跟踪分支。
    fn ensure_local_branch(&self, branch: &str, remote: &str) -> Result<String, Error> {
        let refname = format!("refs/heads/{branch}");
        if self.inner.find_branch(branch, BranchType::Local).is_ok() {
            return Ok(refname);
        }

        let tracking = format!("{remote}/{branch}");
        // 本地和远端跟踪引用都没有时,补一次 fetch —— 目标分支是别人刚推上去、
        // 本仓库还没拉过,这在 CI / 自动化里是常态,不该要求调用方先手动 fetch。
        // 这次 fetch 失败(离线、无此远端)不单独报错:让下面的 BranchNotFound
        // 统一表达「这个分支哪儿都找不到」,错误链更短。
        if self.inner.find_branch(&tracking, BranchType::Remote).is_err() {
            let _ = self.fetch(remote, branch);
        }
        let remote_branch = self
            .inner
            .find_branch(&tracking, BranchType::Remote)
            .map_err(|_| Error::BranchNotFound {
                branch: branch.to_string(),
                remote: remote.to_string(),
            })?;
        let commit = remote_branch
            .get()
            .peel_to_commit()
            .context(GitSnafu { what: format!("peel {tracking}") })?;

        let mut created = self
            .inner
            .branch(branch, &commit, false)
            .context(GitSnafu { what: format!("create branch {branch}") })?;
        created
            .set_upstream(Some(&tracking))
            .context(GitSnafu { what: format!("track {tracking}") })?;
        Ok(refname)
    }

    fn commit(&self, rev: &str) -> Result<git2::Commit<'_>, Error> {
        self.inner
            .revparse_single(rev)
            .and_then(|o| o.peel_to_commit())
            .context(BadRevisionSnafu { rev })
    }
}

/// fetch 用的认证回调。
///
/// 不自己管密钥:依次走 ssh-agent、git 的 credential helper、默认凭证 —— 也就是
/// 复用用户 `git fetch` 本来就在用的那套。libgit2 会带着不同的 `allowed` 反复调用
/// 本回调,所以这里必须按它给的类型分支,不能只认一种。
fn auth_callbacks<'a>() -> RemoteCallbacks<'a> {
    let mut cb = RemoteCallbacks::new();
    cb.credentials(|url, username, allowed| {
        if allowed.contains(CredentialType::SSH_KEY) {
            return Cred::ssh_key_from_agent(username.unwrap_or("git"));
        }
        if allowed.contains(CredentialType::USER_PASS_PLAINTEXT) {
            let cfg = git2::Config::open_default()?;
            return Cred::credential_helper(&cfg, url, username);
        }
        if allowed.contains(CredentialType::DEFAULT) {
            return Cred::default();
        }
        Err(git2::Error::from_str(
            "no supported credential type; tried ssh-agent, credential helper and default",
        ))
    });
    cb
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 建一个临时仓库,提交两次,返回 (仓库目录, 第一次提交, 第二次提交)。
    ///
    /// `Repository` 刻意不返回:`find_commit` 借出的 `Commit` 活得比它久就编不过,
    /// 而各用例本来也只需要目录路径,自己 `Repo::open` 即可。
    fn fixture() -> (tempfile::TempDir, Oid, Oid) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (first, second) = {
            let repo = Repository::init(dir.path()).expect("init");
            let sig = git2::Signature::now("t", "t@example.com").expect("sig");

            let write = |name: &str, body: &str| {
                std::fs::write(dir.path().join(name), body).expect("write");
            };
            let commit = |msg: &str, parents: &[&git2::Commit]| -> Oid {
                let mut index = repo.index().expect("index");
                index
                    .add_all(["*"], git2::IndexAddOption::DEFAULT, None)
                    .expect("add");
                index.write().expect("write index");
                let tree = repo
                    .find_tree(index.write_tree().expect("tree"))
                    .expect("find tree");
                repo.commit(Some("HEAD"), &sig, &sig, msg, &tree, parents)
                    .expect("commit")
            };

            write("a.txt", "one\n");
            let first = commit("first", &[]);
            write("a.txt", "one\ntwo\n");
            let parent = repo.find_commit(first).expect("parent");
            let second = commit("second", &[&parent]);
            (first, second)
        };

        (dir, first, second)
    }

    #[test]
    fn open_rejects_non_repo() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(Repo::open(dir.path()).is_err());
    }

    /// patch 必须是能喂给 `git apply` 的原文,而不是摘要。
    #[test]
    fn diff_renders_a_real_patch() {
        let (dir, first, second) = fixture();
        let repo = Repo::open(dir.path()).expect("open");
        let report = repo
            .diff(&first.to_string(), &second.to_string(), DiffBase::Direct)
            .expect("diff");
        let patch = &report.patch;

        assert!(patch.contains("diff --git a/a.txt b/a.txt"), "{patch}");
        assert!(patch.contains("@@"), "缺 hunk 头: {patch}");
        assert!(patch.contains("+two"), "缺新增行: {patch}");
        // 摘要必须和 patch 出自同一次 diff
        assert_eq!(report.files, vec!["a.txt".to_string()]);
        assert_eq!((report.insertions, report.deletions), (1, 0));
    }

    /// 顺序反过来就是反向 patch,证明 from/to 没被写死。
    #[test]
    fn diff_direction_matters() {
        let (dir, first, second) = fixture();
        let repo = Repo::open(dir.path()).expect("open");
        let reverse = repo
            .diff(&second.to_string(), &first.to_string(), DiffBase::Direct)
            .expect("diff")
            .patch;
        assert!(reverse.contains("-two"), "{reverse}");
    }

    #[test]
    fn diff_rejects_unknown_revision() {
        let (dir, _first, _second) = fixture();
        let repo = Repo::open(dir.path()).expect("open");
        assert!(repo.diff("nope", "HEAD", DiffBase::Direct).is_err());
    }

    /// 切到不存在、且远端也没有的分支,要报 BranchNotFound 而不是默默建一个。
    #[test]
    fn checkout_unknown_branch_fails() {
        let (dir, _first, _second) = fixture();
        let repo = Repo::open(dir.path()).expect("open");
        assert!(matches!(
            repo.checkout("no-such-branch", "origin"),
            Err(Error::BranchNotFound { .. })
        ));
    }
}
