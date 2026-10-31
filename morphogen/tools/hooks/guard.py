#!/usr/bin/env python3
"""Guard hook. Locked. Runs before every tool call (PreToolUse).

Enforces, outside the agent's own judgement:
  * locks:    no tool or shell command may write a locked path
  * budgets:  tool calls per session, gate runs (trials) per day
  * asks:     installs, network side effects and pushes go to the operator
  * autonomy: merges need a passing archive tag, and the operator unless the
              autonomy level in map/intent.md allows the change's lock level

It reads its settings from map/intent.md and the locked paths from
.claude/settings.json, so the operator has one place to change each.
Shell checks are pattern-based: a determined process can evade them, which
is why trials also run in the sandbox and the gate re-checks locked paths.
Standard library only, so it runs before anything is installed.
"""
import datetime
import fnmatch
import json
import os
import re
import shlex
import subprocess
import sys

ROOT = os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
STATE = os.path.join(ROOT, ".morphogen", "state")

READ_ONLY = {"cat", "head", "tail", "less", "more", "grep", "rg", "wc", "diff",
             "ls", "stat", "file", "sha256sum", "md5sum", "find", "tree", "jq",
             "echo", "printf", "true", "test", "[", "evals/run", "evals/gate",
             "./evals/run", "./evals/gate"}
READ_ONLY_GIT = {"diff", "log", "show", "status", "ls-files", "blame", "grep",
                 "rev-parse", "cat-file", "for-each-ref", "tag", "branch"}
INSTALL = re.compile(r"\b(pip3?|uv|pipx|poetry|npm|pnpm|yarn|bun|cargo|go|gem|"
                     r"apt|apt-get|brew|dnf|yum|apk|conda|mamba)\s+(install|add|i|get)\b"
                     r"|\btools/sandbox/build\b|\bdocker\s+(pull|build)\b")
NETWORK = re.compile(r"\bgit\s+push\b|\bcurl\b.*(-X\s*(POST|PUT|PATCH|DELETE)|\s-d\s|--data|\s-F\s)"
                     r"|\b(gh|ssh|scp|sftp|rsync|nc|telnet)\s|\b(npm|cargo|twine)\s+publish\b"
                     r"|\|\s*(sh|bash|zsh)\b")
NEVER = re.compile(r"git\s+push\s.*(--force|-f\b|--delete|:refs/tags/archive)"
                   r"|git\s+tag\s+(-d|--delete)\s+archive/|update-ref\s+-d\s+refs/tags/archive"
                   r"|--no-verify|git\s+(filter-branch|filter-repo)|git\s+notes\s+remove")
# Only evals/gate writes archive tags; a hand-made tag could forge a pass.
FORGE = re.compile(r"git\s+(tag|update-ref|mktag|replace)\b.*archive"
                   r"|refs/tags/archive")
MERGE = re.compile(r"\bgit\s+(merge|rebase|cherry-pick|pull)\b")


def decide(decision, reason):
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": decision,
        "permissionDecisionReason": f"Morphogen guard: {reason}"}}))
    sys.exit(0)


def settings():
    path = os.path.join(ROOT, "map", "intent.md")
    try:
        text = open(path, encoding="utf-8").read()
    except OSError:
        return {}
    m = re.search(r"```morphogen\n(.*?)```", text, re.S)
    out = {}
    for line in (m.group(1).splitlines() if m else []):
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def locked_patterns():
    try:
        deny = json.load(open(os.path.join(ROOT, ".claude", "settings.json")))
        deny = deny.get("permissions", {}).get("deny", [])
    except (OSError, json.JSONDecodeError):
        deny = []
    pats = []
    for rule in deny:
        m = re.fullmatch(r"(?:Edit|Write)\((.+)\)", rule)
        if m:
            pats.append(m.group(1).removeprefix("./"))
    return pats


def is_locked(rel):
    rel = rel.removeprefix("./")
    for p in locked_patterns():
        if p.endswith("/**"):
            if rel == p[:-3] or rel.startswith(p[:-2]):
                return True
        elif rel == p or fnmatch.fnmatch(rel, p):
            return True
    return False


def rel_to_root(path):
    path = os.path.abspath(os.path.join(ROOT, path))
    return os.path.relpath(path, ROOT)


def mentions_locked(segment):
    try:
        words = shlex.split(segment)
    except ValueError:
        words = segment.split()
    for w in words:
        for piece in re.split(r"[=<>]", w):
            if piece and is_locked(rel_to_root(piece)):
                return piece
    for p in locked_patterns():  # e.g. python -c "open('CLAUDE.md','w')"
        bare = p[:-2] if p.endswith("/**") else p  # keep the slash: "evals/"
        if bare and bare in segment:
            return bare
    return None


def read_only(segment):
    if re.search(r"(^|[^0-9&])>{1,2}|\btee\b|\bsed\b.*\s-i|\bperl\b.*\s-i|-exec|-delete", segment):
        return False
    try:
        words = shlex.split(segment)
    except ValueError:
        return False
    if not words:
        return True
    if words[0] in ("python", "python3") and len(words) > 1:
        words = words[1:]
    if words[0] == "git":
        sub = next((w for w in words[1:] if not w.startswith("-")), "")
        if sub in ("tag", "branch") and any(w in ("-d", "-D", "-f", "--delete", "--force") for w in words):
            return False
        return sub in READ_ONLY_GIT
    return words[0] in READ_ONLY


def bump(name):
    """Increment a counter in .morphogen/state and return its new value."""
    os.makedirs(STATE, exist_ok=True)
    path = os.path.join(STATE, name)
    try:
        n = int(open(path).read().strip() or 0)
    except (OSError, ValueError):
        n = 0
    n += 1
    with open(path, "w") as f:
        f.write(str(n))
    return n


def git(*args):
    return subprocess.run(["git", "-C", ROOT, *args], capture_output=True, text=True)


def blueprint_locks():
    """Map path -> lock from the blueprint. Needs PyYAML; None if unavailable."""
    try:
        import yaml
        bp = yaml.safe_load(open(os.path.join(ROOT, "map", "blueprint.yaml")))
    except Exception:
        return None
    out = {}
    for c in (bp.get("components") or []) + (bp.get("slots") or []):
        if isinstance(c, dict) and c.get("path"):
            out[c["path"].rstrip("/")] = c.get("lock", "gated")
    return out


def lock_of(rel, locks):
    if is_locked(rel):
        return "locked"
    best, level = -1, "gated"  # unmapped counts as gated
    for p, lk in locks.items():
        if (rel == p or rel.startswith(p + "/")) and len(p) > best:
            best, level = len(p), lk
    return level


def check_merge(cmd, autonomy):
    words = shlex.split(cmd)
    i = next(k for k, w in enumerate(words) if w in ("merge", "rebase", "cherry-pick", "pull"))
    if words[i] == "pull":
        return "ask", "git pull merges remote work; the operator decides"
    target = next((w for w in words[i + 1:] if not w.startswith("-")), None)
    if words[i] == "merge" and any(w in ("--abort", "--continue") for w in words):
        return None, None
    if not target:
        return "ask", f"git {words[i]} without an explicit branch"
    sha = git("rev-parse", "--verify", target + "^{commit}").stdout.strip()
    if not sha:
        return "ask", f"cannot resolve {target}"
    tags = git("for-each-ref", "refs/tags/archive", "--points-at", sha,
               "--format=%(contents)%00").stdout
    here = git("rev-parse", "HEAD").stdout.strip()
    passed = False
    for chunk in tags.split("\0"):
        try:
            r = json.loads(chunk.strip())
        except json.JSONDecodeError:
            continue
        passed = passed or (r.get("passed") is True and r.get("base") == here)
    if not passed:
        return "deny", (f"{target} ({sha[:8]}) has no passing evals/gate run against the "
                        f"current HEAD ({here[:8]}); check out {target} and run "
                        f"evals/gate --base {git('rev-parse', '--abbrev-ref', 'HEAD').stdout.strip()}")
    if autonomy < 3:
        return "ask", f"autonomy {autonomy}: every merge needs the operator"
    locks = blueprint_locks()
    if locks is None:
        return "ask", "cannot read blueprint lock levels (PyYAML missing); operator decides"
    changed = git("diff", "--name-only", f"HEAD...{sha}", "--", ".").stdout.split()
    prefix = git("rev-parse", "--show-prefix").stdout.strip()
    levels = {lock_of(f[len(prefix):] if f.startswith(prefix) else f, locks) for f in changed}
    if "locked" in levels:
        return "deny", f"{target} changes locked paths"
    if "gated" in levels and autonomy < 4:
        return "ask", f"autonomy {autonomy}: {target} changes gated paths; operator approves"
    return None, None


def main():
    event = json.load(sys.stdin)
    tool = event.get("tool_name", "")
    inp = event.get("tool_input") or {}
    cfg = settings()
    try:
        autonomy = int(cfg.get("autonomy", "2"))
    except ValueError:
        autonomy = 2

    # Budgets apply to every tool call.
    session = re.sub(r"[^\w-]", "", str(event.get("session_id", "unknown")))[:64]
    limit = int(cfg.get("max_tool_calls_per_session", "0") or 0)
    if limit and bump(f"calls-{session}") > limit:
        decide("deny", f"tool-call budget of {limit} for this session is spent; "
                       "the operator can raise max_tool_calls_per_session in map/intent.md")

    if tool in ("Edit", "Write", "NotebookEdit", "MultiEdit"):
        path = inp.get("file_path") or inp.get("notebook_path") or ""
        if path and is_locked(rel_to_root(path)):
            decide("deny", f"{rel_to_root(path)} is locked; propose the change to the operator")
        return

    if tool != "Bash":
        return
    cmd = inp.get("command", "")

    if NEVER.search(cmd):
        decide("deny", "this would destroy history, archive tags or hooks; never allowed")

    if FORGE.search(cmd) and not re.search(r"git\s+(tag\s+(-l|--list|-n)|for-each-ref|show|log)\b", cmd):
        decide("deny", "archive tags are written only by evals/gate")

    for seg in re.split(r"&&|\|\||;|\n|\|(?!\|)", cmd):
        seg = seg.strip()
        hit = mentions_locked(seg) if seg else None
        if hit and not read_only(seg):
            decide("deny", f"command may write locked path {hit}; locked means by any route")

    if re.search(r"(^|[\s;&|])(\./)?evals/gate\b", cmd):
        day = datetime.date.today().isoformat()
        tlimit = int(cfg.get("max_trials_per_day", "0") or 0)
        if tlimit and bump(f"trials-{day}") > tlimit:
            decide("deny", f"trial budget of {tlimit} gate runs for {day} is spent")

    if MERGE.search(cmd):
        d, why = check_merge(cmd, autonomy)
        if d:
            decide(d, why)

    if INSTALL.search(cmd):
        decide("ask", "installs a package or image; the operator approves installs")
    if NETWORK.search(cmd):
        decide("ask", "network side effect (push, post, remote shell); the operator approves")
    if re.search(r"(^|[\s;&|])(\./)?tools/headless\b", cmd) and autonomy < 4:
        decide("deny", f"headless runs need autonomy 4 (now {autonomy})")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:  # fail closed, but to the operator rather than a hard stop
        decide("ask", f"guard error ({e.__class__.__name__}: {e}); operator decides")
