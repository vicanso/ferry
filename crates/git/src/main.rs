//! `ferry-git` 的命令行外壳,三个子命令一一对应 lib 里的三个操作。
//!
//! ```text
//! ferry-git [-C <dir>] checkout <branch> [--remote <name>]
//! ferry-git [-C <dir>] pull              [--remote <name>]
//! ferry-git [-C <dir>] diff <from> <to>  [--merge-base]
//! ```
//!
//! `-C` 取 git 本身的含义:在哪个目录上操作,缺省为当前目录。
//!
//! 参数是手写解析的 —— 就三个子命令、两个开关,为此引入 clap 不划算,ferry 对
//! 依赖一向克制。
//!
//! 典型的「切分支 → 拉最新 → 看变更」串起来:
//!
//! ```text
//! ferry-git -C /srv/app checkout main
//! ferry-git -C /srv/app pull
//! ferry-git -C /srv/app diff HEAD~5 HEAD > changes.patch
//! ```

use std::process::ExitCode;

use ferry_git::{DiffBase, Oid, PullOutcome, Repo};

const USAGE: &str = "\
用法:
    ferry-git [-C <dir>] checkout <branch> [--remote <name>]
    ferry-git [-C <dir>] pull              [--remote <name>]
    ferry-git [-C <dir>] diff <from> <to>  [--merge-base]

选项:
    -C <dir>          在该目录的仓库上操作(缺省:当前目录)
    --remote <name>   远端名(缺省:origin)
    --merge-base      diff 用三点语义(git diff A...B),即从共同祖先算起
";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("ferry-git: {msg}");
            ExitCode::FAILURE
        }
    }
}

/// 抽出带值的选项(`-C x` / `--remote x`),返回它的值;不存在则 None。
fn take_opt(args: &mut Vec<String>, name: &str) -> Result<Option<String>, String> {
    let Some(pos) = args.iter().position(|a| a == name) else {
        return Ok(None);
    };
    if pos + 1 >= args.len() {
        return Err(format!("{name} 缺少参数值"));
    }
    args.remove(pos);
    Ok(Some(args.remove(pos)))
}

/// 抽出布尔开关,返回它是否出现过。
fn take_flag(args: &mut Vec<String>, name: &str) -> bool {
    match args.iter().position(|a| a == name) {
        Some(pos) => {
            args.remove(pos);
            true
        }
        None => false,
    }
}

fn run() -> Result<(), String> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        return Ok(());
    }

    // 选项先摘掉,剩下的就都是位置参数,顺序不敏感
    let dir = take_opt(&mut args, "-C")?.unwrap_or_else(|| ".".to_string());
    let remote = take_opt(&mut args, "--remote")?.unwrap_or_else(|| "origin".to_string());
    let merge_base = take_flag(&mut args, "--merge-base");

    let repo = Repo::open(&dir).map_err(|e| format!("{e}"))?;
    let cmd = args.first().cloned().unwrap_or_default();
    let rest = &args[1.min(args.len())..];

    match cmd.as_str() {
        "checkout" => {
            let [branch] = rest else {
                return Err(format!("checkout 需要且仅需要一个分支名\n\n{USAGE}"));
            };
            repo.checkout(branch, &remote).map_err(|e| format!("{e}"))?;
            eprintln!("已切换到 {branch}");
        }
        "pull" => {
            if !rest.is_empty() {
                return Err(format!("pull 不接受位置参数\n\n{USAGE}"));
            }
            match repo.pull(&remote).map_err(|e| format!("{e}"))? {
                PullOutcome::UpToDate => eprintln!("已是最新"),
                PullOutcome::FastForward { from, to } => {
                    eprintln!("快进 {} → {}", short(from), short(to))
                }
                // 分叉不是崩溃,但也绝不能当成功:退出码要非 0,否则脚本会照常往下跑
                PullOutcome::Diverged { local, remote } => {
                    return Err(format!(
                        "本地与远端已分叉(本地 {} / 远端 {});需要人工 merge 或 rebase",
                        short(local),
                        short(remote)
                    ));
                }
            }
        }
        "diff" => {
            let [from, to] = rest else {
                return Err(format!("diff 需要两个 revision\n\n{USAGE}"));
            };
            let base = if merge_base {
                DiffBase::MergeBase
            } else {
                DiffBase::Direct
            };
            // patch 走 stdout(方便重定向成 .patch),进度和错误走 stderr
            let report = repo.diff(from, to, base).map_err(|e| format!("{e}"))?;
            print!("{}", report.patch);
        }
        other => return Err(format!("未知子命令 {other:?}\n\n{USAGE}")),
    }
    Ok(())
}

fn short(oid: Oid) -> String {
    oid.to_string()[..7].to_string()
}
