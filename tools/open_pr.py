#!/usr/bin/env python3
"""
tools/open_pr.py — open a PR to a third-party repo.

Workflow:
  1. fork the upstream repo into our account (skip if we already have a fork)
  2. clone (or reuse) the upstream, apply our patch, commit
  3. push the branch to our fork
  4. open the PR through the REST API (gh pr create needs a repo cwd, the
     API does not)

Usage:
  python3 tools/open_pr.py <upstream> <base_branch> <branch> <title> <patch_file> [--pr-body file]
"""
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time

SCRATCH = pathlib.Path("/root/.hermes/cache/scratch")
WORKROOT = SCRATCH / "pr-work"


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


def gh(tok: str, args: list, stdin: str | None = None, timeout: int = 240):
    return subprocess.run(
        ["gh", "api"] + args,
        capture_output=True, text=True, timeout=timeout, env=gh_env(tok),
        input=stdin,
    )


def ensure_fork(tok: str, upstream: str) -> str:
    """Fork upstream into our account; return 'owner/repo' of the fork."""
    owner = upstream.split("/")[0]
    name = upstream.split("/")[1]
    fork = f"Saiop68/{name}"

    # already forked?
    r = gh(tok, [f"repos/{fork}"])
    if r.returncode == 0:
        print(f"  fork exists: {fork}")
        return fork

    r = gh(tok, ["--method", "POST", f"repos/{upstream}/forks", "--jq", ".full_name"])
    if r.returncode != 0:
        print(f"  fork failed: {r.stderr[:200]}")
        return ""
    print(f"  forked -> {r.stdout.strip()}")

    # wait for it to materialise
    for _ in range(20):
        time.sleep(2)
        r2 = gh(tok, [f"repos/{fork}", "--jq", ".default_branch"])
        if r2.returncode == 0:
            return fork
    return fork


def main() -> int:
    if len(sys.argv) < 6:
        print(__doc__)
        return 2

    upstream, base, branch, title, patch_file = sys.argv[1:6]
    pr_body_file = None
    if "--pr-body" in sys.argv:
        pr_body_file = sys.argv[sys.argv.index("--pr-body") + 1]

    tok = token()
    fork = ensure_fork(tok, upstream)
    if not fork:
        return 1

    WORKROOT.mkdir(parents=True, exist_ok=True)
    work = WORKROOT / upstream.replace("/", "__")
    if work.exists():
        shutil.rmtree(work)
    r = subprocess.run(
        ["git", "clone", "--depth", "1", f"https://github.com/{upstream}.git", str(work)],
        capture_output=True, text=True, timeout=300,
    )
    if r.returncode != 0:
        print(f"  clone failed: {r.stderr[:200]}")
        return 1

    def g(args, t=240):
        return subprocess.run(["git"] + args, capture_output=True, text=True,
                              timeout=t, cwd=str(work))

    g(["checkout", "-b", branch])
    g(["config", "user.name", "Saiop68"])
    g(["config", "user.email", "1391639072405196932@users.noreply.github.com"])

    # apply the patch: a python file that edits files in cwd, or a diff
    patch = pathlib.Path(patch_file)
    if patch.suffix == ".py":
        r2 = subprocess.run(["python3", str(patch.resolve())], cwd=str(work),
                            capture_output=True, text=True, timeout=120)
        if r2.returncode != 0:
            print(f"  patch script failed: {r2.stderr[:300]}")
            return 1
    else:
        r2 = subprocess.run(["git", "apply", str(patch.resolve())], cwd=str(work),
                            capture_output=True, text=True, timeout=120)
        if r2.returncode != 0:
            print(f"  git apply failed: {r2.stderr[:300]}")
            return 1

    g(["add", "-A"])
    status = g(["status", "--short"]).stdout.strip()
    if not status:
        print("  no changes to commit")
        return 1
    print("  changes:", status.replace("\n", " | ")[:160])

    r3 = subprocess.run(
        ["git", "-c", "user.name=Saiop68",
         "-c", "user.email=1391639072405196932@users.noreply.github.com",
         "commit", "-q", "-m", title],
        cwd=str(work), capture_output=True, text=True, timeout=120,
    )
    if r3.returncode != 0:
        print(f"  commit failed: {r3.stderr[:200]}")
        return 1

    g(["remote", "set-url", "origin", f"git@github.com:{fork}.git"])
    r4 = g(["push", "-u", "origin", branch], t=300)
    if r4.returncode != 0:
        print(f"  push failed: {(r4.stdout + r4.stderr)[-200:]}")
        return 1

    body = ""
    if pr_body_file:
        body = pathlib.Path(pr_body_file).read_text()

    payload = {
        "title": title,
        "head": f"Saiop68:{branch}",
        "base": base,
        "body": body,
    }
    pf = SCRATCH / f"pr-{upstream.replace('/', '__')}.json"
    pf.write_text(json.dumps(payload))

    r5 = gh(tok, ["--method", "POST", f"repos/{upstream}/pulls",
                  "--input", str(pf), "--jq", ".html_url"])
    if r5.returncode != 0:
        print(f"  PR failed: {r5.stderr[:300]}")
        return 1
    print(f"  PR: {r5.stdout.strip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
