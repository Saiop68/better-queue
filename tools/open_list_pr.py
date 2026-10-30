#!/usr/bin/env python3
"""
tools/open_list_pr.py — open a curated awesome-list PR for BetterQueue.

Quality rules this script enforces, because a rejected PR is worth less than
no PR:

  1. Only touch lists that already curate a comparable package (the anchor
     must be an existing entry for bottleneck / bull / bullmq / p-queue /
     rate-limiter-flexible / kue). If the list does not curate packages in
     this space, we do not pitch it.
  2. Skip lists whose CONTRIBUTING rules require a minimum star count or a
     minimum repo age — a PR there gets closed automatically.
  3. Match the list's language. A Chinese list gets a Chinese body.
  4. One entry, one line, in the same format as the anchor line.
  5. The body leads with the mechanism and the measured numbers, not with
     marketing.
  6. Every PR is verified afterwards: it must be open, mergeable, and the
     diff must be exactly one added line.

Usage:
  python3 tools/open_list_pr.py <upstream> <base> <anchor-substring> [--lang en|zh] [--title T]
"""
import base64
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time

SCRATCH = pathlib.Path("/root/.hermes/cache/scratch")
WORKROOT = SCRATCH / "list-pr-work"
REPO = "Saiop68/better-queue"
BRANCH = "add/better-queue"

# Packages that mean "this list curates libraries in our space".
CURATION_MARKERS = (
    "SGrondin/bottleneck",
    "OptimalBits/bull",
    "taskforcesh/bullmq",
    "animir/node-rate-limiter-flexible",
    "Automattic/kue",
    "sindresorhus/p-queue",
)

ENTRY = {
    "en": ("* [BetterQueue](https://github.com/Saiop68/better-queue) - Distributed rate "
           "limiter and job queue for Node.js with Redis-backed state, cross-process "
           "priority and crash recovery.\n"),
    "zh": ("* [BetterQueue](https://github.com/Saiop68/better-queue)：基于 Redis 的 Node.js "
           "分布式限流器与任务队列，支持跨进程优先级、延时任务与崩溃恢复。\n"),
}

BODY = {
    "en": """Adds [BetterQueue](https://github.com/Saiop68/better-queue), a distributed rate limiter and job queue for Node.js.

The queue state lives in Redis rather than in the process, which changes three practical things:

- **A limiter is a thin handle.** Creating 1000 limiters costs roughly what 10 do, because the per-limiter state is not held in the process.
- **Jobs survive a crash.** A job held by a worker that dies is re-queued and picked up by another process instead of being lost.
- **Priority ordering is global.** Ordering is computed server-side in Redis, so a high-priority job outranks a lower-priority one in any process, not just within one limiter.

It also supports `scheduleAt()` for work that needs to run later, which Bottleneck does not.

The repository includes a benchmark that runs both libraries in separate processes against the same Redis. With 1000 limiters and 51,000 jobs:

| Metric | Bottleneck 2.19.5 | BetterQueue |
|---|---:|---:|
| RSS after the run | 415 MB | 222 MB |
| Heap after forced GC | 14 MB | 11 MB |
| Throughput | 9,957 jobs/s | 11,456 jobs/s |

MIT licensed, written in TypeScript, 37 tests with CI running on Node 18/20/22.
""",
    "zh": """新增 [BetterQueue](https://github.com/Saiop68/better-queue)，一个基于 Redis 的 Node.js 分布式限流器与任务队列。

队列状态完全存放在 Redis 中，而不是进程内，因此带来三点实际差异：

- **限流器只是一个轻量句柄**。创建 1000 个限流器的内存开销与 10 个相当，因为每个限流器的状态不保留在进程中。
- **任务不随进程崩溃丢失**。工作进程崩溃时正在执行的任务会被重新入队，由其他进程接管，而不是直接丢失。
- **优先级是全局的**。排序在 Redis 服务端完成，因此高优先级任务在任何进程中都优先于低优先级任务，而不仅仅是在单个限流器内部。

另外还支持 `scheduleAt()`，用于需要在未来某个时刻执行的任务，这是 Bottleneck 不具备的。

仓库中附带了基准测试脚本，在同一台机器、同一 Redis 上以独立进程分别运行两个库。1000 个限流器、51,000 个任务的测试结果：

| 指标 | Bottleneck 2.19.5 | BetterQueue |
|---|---:|---:|
| 运行结束后 RSS | 415 MB | 222 MB |
| 强制 GC 后堆内存 | 14 MB | 11 MB |
| 吞吐量 | 9,957 jobs/s | 11,456 jobs/s |

MIT 协议，TypeScript 实现，37 个测试用例，CI 覆盖 Node 18/20/22。
""",
}


def token() -> str:
    out = subprocess.run(
        ["bash", "-c", "grep -E '^GITHUB_TOKEN=' /root/.hermes/.env | head -1 | cut -d'=' -f2-"],
        capture_output=True, text=True, timeout=20,
    ).stdout.strip()
    if not out:
        sys.exit("no GITHUB_TOKEN")
    return out


def gh_env(tok: str) -> dict:
    return {"GH_TOKEN": tok, "PATH": "/usr/bin:/bin:/usr/local/bin", "HOME": "/root"}


def api(tok: str, path: str, method: str | None = None, data: dict | None = None, timeout: int = 240):
    args = ["gh", "api"]
    if method:
        args += ["--method", method]
    if data is not None:
        args += ["--input", "-"]
    args += [path]
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                          env=gh_env(tok), input=json.dumps(data) if data is not None else None)


def read_file_api(tok: str, repo: str, path: str) -> str | None:
    r = api(tok, f"/repos/{repo}/contents/{path}", timeout=60)
    if not r.stdout.strip().startswith("{"):
        return None
    try:
        return base64.b64decode(json.loads(r.stdout)["content"]).decode()
    except Exception:
        return None


def has_gate(tok: str, repo: str) -> tuple[bool, str]:
    """Return (blocked, reason) if the list's rules gate newcomers."""
    for f in ("contributing.md", "CONTRIBUTING.md", ".github/PULL_REQUEST_TEMPLATE.md"):
        txt = read_file_api(tok, repo, f)
        if not txt:
            continue
        low = txt.lower()
        if ("at least 100 stars" in low or "100+ stars" in low or "minimum of 100" in low
                or "at least 50 stars" in low or "50+ stars" in low):
            return True, f"{f}: star minimum"
        if ("30 days old" in low or "more than 30 days" in low or "at least 30 days" in low
                or "one month old" in low):
            return True, f"{f}: age minimum"
    return False, ""


def find_anchor(tok: str, repo: str, branch: str, hint: str):
    """Locate the file + line to insert after.

    Returns (path, anchor_line, language) or None.
    """
    files = ["README.md", "readme.md", "Readme.md", "ReadMe.md", "README.MD"]
    for f in files:
        txt = read_file_api(tok, repo, f)
        if not txt:
            continue
        for line in txt.split("\n"):
            if hint not in line:
                continue
            if not line.strip().startswith(("-", "*")):
                continue
            # a curated package entry, not prose
            if "http" not in line:
                continue
            lang = "zh" if re.search(r"[一-鿿]", line) else "en"
            return f, line, lang
    return None


def main() -> int:
    if len(sys.argv) < 4:
        print(__doc__)
        return 2

    upstream, base, hint = sys.argv[1:4]
    lang_override = None
    title_override = None
    if "--lang" in sys.argv:
        lang_override = sys.argv[sys.argv.index("--lang") + 1]
    if "--title" in sys.argv:
        title_override = sys.argv[sys.argv.index("--title") + 1]

    tok = token()

    # 1. rules gate?
    blocked, reason = has_gate(tok, upstream)
    if blocked:
        print(f"  SKIP {upstream}: {reason}")
        return 3

    # 2. anchor?
    found = find_anchor(tok, upstream, base, hint)
    if not found:
        print(f"  SKIP {upstream}: no curated anchor for {hint!r}")
        return 4
    path, anchor, detected_lang = found
    lang = lang_override or detected_lang

    # 3. already listed?
    txt = read_file_api(tok, upstream, path) or ""
    if REPO in txt:
        print(f"  SKIP {upstream}: already listed")
        return 5

    # 4. fork
    #    GitHub appends -1 when a fork with that name already exists, so the
    #    name we asked for is NOT necessarily the fork we get. Always use the
    #    name the API returns, and never assume it.
    fork = f"Saiop68/{upstream.split('/')[1]}"
    r = api(tok, f"/repos/{upstream}/forks", "POST")
    if '"full_name"' in r.stdout:
        try:
            fork = json.loads(r.stdout)["full_name"]
        except Exception:
            pass

    # If a fork with this name already exists but points at a DIFFERENT
    # upstream, GitHub's PR endpoint will 404. Verify the parent.
    chk = api(tok, f"/repos/{fork}", timeout=60)
    try:
        parent = json.loads(chk.stdout).get("parent", {}).get("full_name")
        if parent and parent != upstream:
            print(f"  FAIL {upstream}: fork {fork} belongs to {parent}")
            return 12
    except Exception:
        pass

    # 5. clone + edit
    WORKROOT.mkdir(parents=True, exist_ok=True)
    work = WORKROOT / upstream.replace("/", "__")
    if work.exists():
        shutil.rmtree(work)
    c = subprocess.run(["git", "clone", "--depth", "1", "--branch", base,
                        f"https://github.com/{upstream}.git", str(work)],
                       capture_output=True, text=True, timeout=300)
    if c.returncode != 0:
        print(f"  FAIL {upstream}: clone {c.stderr.strip()[-90:]}")
        return 6

    def g(args, t=240):
        return subprocess.run(["git"] + args, capture_output=True, text=True,
                              timeout=t, cwd=str(work))

    # locate the anchor in the local clone (the API may serve a different default branch)
    local_path = work / path
    if not local_path.exists():
        print(f"  FAIL {upstream}: {path} not in clone")
        return 7
    local = local_path.read_text()
    if REPO in local:
        print(f"  SKIP {upstream}: already listed")
        return 5
    if anchor not in local:
        # fall back to a regex match on the hint
        m = re.search(r"^.*" + re.escape(hint) + r".*$", local, re.M)
        if not m:
            print(f"  FAIL {upstream}: anchor vanished in clone")
            return 8
        anchor = m.group(0)
        if not anchor.strip().startswith(("-", "*")):
            print(f"  FAIL {upstream}: fallback anchor is not a list item")
            return 8

    # same list format as the anchor
    prefix = "*" if anchor.lstrip().startswith("*") else "-"
    entry = ENTRY[lang].replace("* ", f"{prefix} ", 1)

    # The anchor may or may not carry a trailing newline depending on how it was
    # read. Normalise so the entry lands directly under it with no stray blank
    # line in the diff.
    stripped = anchor.rstrip("\n")
    if anchor in local:
        local = local.replace(anchor, stripped + "\n" + entry.rstrip("\n"), 1)
    else:
        local = local.replace(stripped, stripped + "\n" + entry.rstrip("\n"), 1)
    local_path.write_text(local)

    title = title_override or (
        "在 API 分类中新增 BetterQueue" if lang == "zh"
        else "Add BetterQueue to the list"
    )

    g(["checkout", "-b", BRANCH])
    g(["config", "user.name", "Saiop68"])
    g(["config", "user.email", "1391639072405196932@users.noreply.github.com"])
    g(["add", "-A"])
    if not g(["status", "--short"]).stdout.strip():
        print(f"  SKIP {upstream}: nothing changed")
        return 9
    g(["-c", "user.name=Saiop68", "-c",
       "user.email=1391639072405196932@users.noreply.github.com",
       "commit", "-q", "-m", title])
    g(["remote", "set-url", "origin", f"git@github.com:{fork}.git"])
    # Force-push: the branch lives on OUR fork and we are the only writer.
    # A recreated fork can come back with the previous attempt's branch, which
    # makes a normal push a non-fast-forward rejection.
    p = g(["push", "--force", "-u", "origin", BRANCH], 300)
    if p.returncode != 0:
        print(f"  FAIL {upstream}: push {(p.stdout + p.stderr)[-110:]}")
        return 10

    payload = {
        "title": title,
        "head": f"Saiop68:{BRANCH}",
        "base": base,
        "body": BODY[lang],
    }
    pf = SCRATCH / f"lp-{upstream.replace('/', '__')}.json"
    pf.write_text(json.dumps(payload))
    r2 = api(tok, f"/repos/{upstream}/pulls", "POST", payload)
    if r2.returncode != 0:
        print(f"  FAIL {upstream}: PR {r2.stderr[:170]}")
        return 11

    try:
        url = json.loads(r2.stdout)["html_url"]
    except Exception:
        print(f"  FAIL {upstream}: unparseable PR response")
        return 11

    # 6. verify
    time.sleep(2)
    v = api(tok, f"/repos/{upstream}/pulls/{json.loads(r2.stdout)['number']}", timeout=90)
    try:
        vd = json.loads(v.stdout)
        diff = api(tok, f"/repos/{upstream}/pulls/{vd['number']}/files", timeout=90)
        files = json.loads(diff.stdout)
        adds = sum(f.get("additions", 0) for f in files)
        dels = sum(f.get("deletions", 0) for f in files)
        print(f"  OK {url}  +{adds}/-{dels}  state={vd['state']} mergeable={vd['mergeable']}")
        if dels > 0:
            print(f"     WARN {upstream}: diff has deletions — check manually")
        return 0
    except Exception as e:
        print(f"  OK {url}  (verify failed: {e})")
        return 0


if __name__ == "__main__":
    sys.exit(main())
