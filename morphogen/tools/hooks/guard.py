#!/usr/bin/env python3
"""Guard. Locked. One script, four modes, registered in .claude/settings.json:

    guard.py pre         PreToolUse, every tool
    guard.py post        PostToolUse, Bash
    guard.py session     SessionStart: tells the agent its dials and state
    guard.py statusline  the operator's status line

Enforces, outside the agent's own judgement:
  * locks     File tools may not write locked paths. Shell commands that name a
              locked path must be read-only. Any shell command that changes a
              locked file anyway (git apply, a script, plumbing) is undone
              afterwards from a snapshot taken just before it ran.
  * trunk     No commits, resets or ref moves on the trunk branch; it changes
              only by merging a commit that passed evals/gate against its tip.
  * autonomy  Who approves that merge depends on the autonomy level and the
              lock levels of the files it changes.
  * asks      Installs, network side effects and pushes go to the operator.
  * budgets   Tool calls per session. (Trials per day: evals/gate counts them.)

Settings come from the ```morphogen block in map/intent.md, locked paths from
the deny rules in .claude/settings.json. Pattern checks on shell commands can be
evaded by a determined process; the snapshot catches what they miss, and
inside the devcontainer the judge and this file are mounted read-only.
Standard library only, so it runs before anything is installed.
"""
import base64
import datetime
import fnmatch
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys

ROOT = os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()

READ_ONLY = {"cat", "head", "tail", "less", "more", "grep", "rg", "wc", "diff",
             "cmp", "ls", "find", "stat", "file", "sha256sum", "md5sum", "tree", "jq",
             "echo", "printf", "true", "test", "[", "evals/run", "evals/gate",
             "./evals/run", "./evals/gate", "tools/sandbox/run", "./tools/sandbox/run"}
READ_ONLY_GIT = {"diff", "log", "show", "status", "ls-files", "ls-tree", "blame",
                 "grep", "rev-parse", "cat-file", "for-each-ref", "merge-base",
                 "describe", "shortlog", "tag", "branch"}
INSTALL = re.compile(r"\b(pip3?|uv|pipx|poetry|npm|pnpm|yarn|bun|cargo|go|gem|"
                     r"apt|apt-get|brew|dnf|yum|apk|conda|mamba)\s+(install|add|i|get)\b"
                     r"|\btools/sandbox/build\b|\bdocker\s+(pull|build)\b")
NETWORK = re.compile(r"\bgit\s+push\b|\bcurl\b.*(-X\s*(POST|PUT|PATCH|DELETE)|\s-d\s|--data|\s-F\s)"
                     r"|\b(gh|ssh|scp|sftp|rsync|nc|telnet)\s|\b(npm|cargo|twine)\s+publish\b"
                     r"|\|\s*(sh|bash|zsh)\b")
NEVER = re.compile(r"git\s+push\s.*(--force|-f\b|--delete|:refs/tags/archive)"
                   r"|git\s+tag\s+(-d|--delete)\s+archive/|update-ref\s+-d\s+refs/tags/archive"
                   r"|--no-verify|git\s+(filter-branch|filter-repo)|git\s+notes\s+remove"
                   r"|core\.hooksPath|core\.fsmonitor|core\.sshCommand|\bfilter\.\w+\.(smudge|clean)")
# Only evals/gate writes archive tags; a hand-made tag could forge a pass.
FORGE = re.compile(r"git\s+(tag|update-ref|mktag|replace)\b.*archive|refs/tags/archive")
FORGE_OK = re.compile(r"git\s+(tag\s+(-l|--list|-n)|for-each-ref|show|log)\b")
TRUNK_WRITE = re.compile(r"\bgit\s+(commit|reset|revert|am|cherry-pick|rebase|stash\s+(pop|apply))\b")
VALUE_OPTS = {"-m", "-s", "-X", "-F", "--message", "--strategy", "--strategy-option", "--file"}


# ---------------------------------------------------------------- settings

def dials():
    try:
        text = open(os.path.join(ROOT, "map", "intent.md"), encoding="utf-8").read()
    except OSError:
        text = ""
    m = re.search(r"```morphogen\n(.*?)```", text, re.S)
    out = {}
    for line in (m.group(1).splitlines() if m else []):
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    out["_purpose_set"] = "To be written by the operator" not in text
    return out


def num(cfg, key, default=0):
    try:
        return int(float(cfg.get(key, default)))
    except ValueError:
        return default


def patterns():
    """Locked patterns from the deny rules, as absolute paths."""
    try:
        rules = json.load(open(os.path.join(ROOT, ".claude", "settings.json")))
        rules = rules.get("permissions", {}).get("deny", [])
    except (OSError, json.JSONDecodeError):
        rules = []
    out = []
    for rule in rules:
        m = re.fullmatch(r"(?:Edit|Write)\((.+)\)", rule)
        if not m:
            continue
        p = m.group(1)
        tree = p.endswith("/**")
        p = p.removesuffix("/**")
        if p.startswith("~/"):
            p = os.path.expanduser(p)
        elif p.startswith("//"):
            p = p[1:]
        else:
            p = os.path.join(ROOT, p.removeprefix("./"))
        out.append((m.group(1), os.path.normpath(p) + ("/**" if tree else "")))
    return out


def is_locked(abspath):
    abspath = os.path.normpath(abspath)
    for _, p in patterns():
        if p.endswith("/**"):
            base = p[:-3].rstrip("/")
            if abspath == base or abspath.startswith(base + "/"):
                return True
        elif abspath == p or fnmatch.fnmatch(abspath, p):
            return True
    return False


def rel(abspath):
    return os.path.relpath(abspath, ROOT)


# ---------------------------------------------------------------- shell checks

def mentions_locked(segment, cwd):
    try:
        words = shlex.split(segment)
    except ValueError:
        words = segment.split()
    for w in words:
        for piece in re.split(r"[=<>]", w):
            if piece and is_locked(os.path.join(cwd, os.path.expanduser(piece))):
                return piece
    for raw, p in patterns():  # catches paths inside code: python -c "open('CLAUDE.md','w')"
        for needle in {raw.removesuffix("**"), p.removesuffix("**")}:
            needle = needle.removeprefix("./")
            if needle and needle in segment:
                return needle
    return None


def read_only(segment):
    s = re.sub(r"\d*>&\d+|\d*>\s*/dev/null", "", segment)  # fd juggling is not a write
    if re.search(r">|\btee\b|\bsed\b.*\s-i|\bperl\b.*\s-i|-exec|-delete|\bdd\b", s):
        return False
    try:
        words = shlex.split(s)
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


def git(*args):
    return subprocess.run(["git", "-C", ROOT, *args], capture_output=True, text=True)


def current_branch():
    return git("symbolic-ref", "--quiet", "--short", "HEAD").stdout.strip()


def trunk_ref_move(cmd, trunk):
    t = re.escape(trunk)
    return re.search(rf"update-ref\s+(refs/heads/)?{t}\b"
                     rf"|\bbranch\s+(-f|--force|-M|-m|-D|-d|--delete)\b.*\b{t}\b"
                     rf"|\b(checkout\s+-B|switch\s+(-C|--force-create))\s+{t}\b"
                     rf"|:(refs/heads/)?{t}\b", cmd)


# ---------------------------------------------------------------- merges

def blueprint_locks():
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


def lock_of(path, locks):
    if is_locked(os.path.join(ROOT, path)):
        return "locked"
    best, level = -1, "gated"  # unmapped counts as gated
    for p, lk in locks.items():
        if (path == p or path.startswith(p + "/")) and len(p) > best:
            best, level = len(p), lk
    return level


def check_merge(cmd, autonomy, trunk):
    words = shlex.split(cmd)
    i = next(k for k, w in enumerate(words) if w in ("merge", "pull"))
    if words[i] == "pull":
        return "ask", f"git pull on {trunk} merges remote work; the operator decides"
    if any(w in ("--abort", "--quit") for w in words):
        return None, None
    target, skip = None, False
    for w in words[i + 1:]:
        if skip:
            skip = False
        elif w in VALUE_OPTS:
            skip = True
        elif not w.startswith("-"):
            target = w
            break
    if not target:
        return "ask", "git merge without an explicit branch"
    sha = git("rev-parse", "--verify", target + "^{commit}").stdout.strip()
    here = git("rev-parse", "HEAD").stdout.strip()
    if not sha:
        return "ask", f"cannot resolve {target}"
    passed = False
    out = git("for-each-ref", "refs/tags/archive", "--points-at", sha, "--format=%(contents)%00").stdout
    for chunk in out.split("\0"):
        try:
            r = json.loads(chunk.strip())
        except json.JSONDecodeError:
            continue
        passed = passed or (r.get("passed") is True and r.get("base") == here)
    if not passed:
        return "deny", (f"{target} ({sha[:8]}) has no passing evals/gate run against {trunk} "
                        f"as it is now ({here[:8]}). Check out {target}, merge or rebase onto "
                        f"{trunk} if needed, and run evals/gate --base {trunk}")
    if autonomy < 3:
        return "ask", f"autonomy {autonomy}: every merge needs the operator"
    locks = blueprint_locks()
    if locks is None:
        return "ask", "cannot read blueprint lock levels (PyYAML missing); operator decides"
    prefix = git("rev-parse", "--show-prefix").stdout.strip()
    changed = git("diff", "--name-only", f"HEAD...{sha}", "--", ".").stdout.split()
    levels = {lock_of(f.removeprefix(prefix), locks) for f in changed}
    if "locked" in levels:
        return "deny", f"{target} changes locked paths"
    if "gated" in levels and autonomy < 4:
        return "ask", f"autonomy {autonomy}: {target} changes gated paths; operator approves"
    return None, None


# ---------------------------------------------------------------- snapshots

def state_dir():
    d = os.path.join(ROOT, ".morphogen", "state")
    os.makedirs(d, exist_ok=True)
    return d


def locked_files():
    """Existing files under locked patterns. Skips .morphogen (hook state)."""
    own = os.path.join(ROOT, ".morphogen")
    files = set()
    for _, p in patterns():
        if p.endswith("/**"):
            base = p[:-3].rstrip("/")
            if base == own or not base.startswith(ROOT):
                continue
            for d, _, names in os.walk(base):
                if "__pycache__" not in d:
                    files.update(os.path.join(d, n) for n in names)
        elif os.path.isfile(p):
            files.add(p)
    return files


def snapshot(key, trunk, trunk_ok):
    snap = {}
    for f in locked_files():
        try:
            snap[f] = base64.b64encode(open(f, "rb").read()).decode()
        except OSError:
            pass
    sha = git("rev-parse", "--verify", "--quiet", f"refs/heads/{trunk}").stdout.strip()
    with open(os.path.join(state_dir(), f"snap-{key}.json"), "w") as fh:
        json.dump({"files": snap, "trunk": trunk, "sha": sha, "trunk_ok": trunk_ok}, fh)


def restore_trunk(state):
    """Put the trunk back if it moved without a checked merge; keep the work."""
    trunk, old = state.get("trunk"), state.get("sha")
    if not trunk or not old or state.get("trunk_ok"):
        return None
    new = git("rev-parse", "--verify", "--quiet", f"refs/heads/{trunk}").stdout.strip()
    if new == old:
        return None
    rescue = f"rescued/{new[:8]}"
    git("branch", rescue, new)
    if current_branch() == trunk:
        git("reset", "--keep", old)
    else:
        git("update-ref", f"refs/heads/{trunk}", old, new)
    return f"{trunk} moved without a gated merge, so it was put back to {old[:8]}; the new commits are on branch {rescue}"


def restore(key):
    path = os.path.join(state_dir(), f"snap-{key}.json")
    try:
        state = json.load(open(path))
    except (OSError, json.JSONDecodeError):
        return []
    os.remove(path)
    snap = state.get("files", {})
    undone = []
    note = restore_trunk(state)
    if note:
        undone.append(note)
    for f in locked_files() - set(snap):
        os.remove(f)
        undone.append(f"{rel(f)} (created)")
    for f, data in snap.items():
        old = base64.b64decode(data)
        try:
            same = open(f, "rb").read() == old
        except OSError:
            same = False
        if not same:
            os.makedirs(os.path.dirname(f), exist_ok=True)
            with open(f, "wb") as fh:
                fh.write(old)
            undone.append(rel(f))
    return undone


# ---------------------------------------------------------------- counters

def counter(name, bump=False):
    path = os.path.join(state_dir(), name)
    try:
        n = int(open(path).read().strip() or 0)
    except (OSError, ValueError):
        n = 0
    if bump:
        n += 1
        with open(path, "w") as fh:
            fh.write(str(n))
    return n


def session_key(event):
    return re.sub(r"[^\w-]", "", str(event.get("session_id", "unknown")))[:64]


def call_key(event):
    raw = str(event.get("tool_use_id") or event.get("session_id", "x"))
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------- modes

def decide(decision, reason):
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": decision,
        "permissionDecisionReason": f"Morphogen guard: {reason}"}}))
    sys.exit(0)


def pre(event):
    tool = event.get("tool_name", "")
    inp = event.get("tool_input") or {}
    cwd = event.get("cwd") or ROOT
    cfg = dials()
    autonomy = num(cfg, "autonomy", 2)
    trunk = cfg.get("trunk", "main")

    limit = num(cfg, "max_tool_calls_per_session")
    if limit and counter(f"calls-{session_key(event)}", bump=True) > limit:
        decide("deny", f"tool-call budget of {limit} for this session is spent; "
                       "the operator can raise max_tool_calls_per_session in map/intent.md")

    if tool in ("Edit", "Write", "NotebookEdit", "MultiEdit"):
        path = inp.get("file_path") or inp.get("notebook_path") or ""
        if path and is_locked(os.path.join(cwd, os.path.expanduser(path))):
            decide("deny", f"{path} is locked; propose the change to the operator")
        return
    if tool != "Bash":
        return
    cmd = inp.get("command", "")

    if NEVER.search(cmd):
        decide("deny", "this would destroy history or archive tags, or run code outside the "
                       "session through git config or hooks; never allowed")
    if FORGE.search(cmd) and not FORGE_OK.search(cmd):
        decide("deny", "archive tags are written only by evals/gate")
    for seg in re.split(r"&&|\|\||;|\n|\|", cmd):
        seg = seg.strip()
        if re.match(r"git\s+commit\b", seg):
            continue  # a commit writes no working files; the gate checks what it holds
        hit = mentions_locked(seg, cwd) if seg else None
        if hit and not read_only(seg):
            decide("deny", f"command may write locked path {hit}; locked means by any route")

    if trunk_ref_move(cmd, trunk):
        decide("deny", f"{trunk} moves only by a gated merge")
    switches = re.search(rf"\bgit\s+(checkout|switch)\s+{re.escape(trunk)}\b", cmd)
    on_trunk = current_branch() == trunk or bool(switches)
    if on_trunk and TRUNK_WRITE.search(cmd):
        decide("deny", f"no direct changes on {trunk}; work on a branch: "
                       f"git checkout -b evolve/<component>-<gap>")
    merging = on_trunk and re.search(r"\bgit\s+(merge|pull)(?![\w-])", cmd)
    if merging:
        d, why = check_merge(cmd, autonomy, trunk)
        if d == "deny":
            decide(d, why)

    if INSTALL.search(cmd):
        decide("ask", "installs a package or image; the operator approves installs")
    if NETWORK.search(cmd):
        decide("ask", "network side effect (push, post, remote shell); the operator approves")
    if re.search(r"(^|[\s;&|])(\./)?tools/headless\b", cmd) and autonomy < 4:
        decide("deny", f"headless runs need autonomy 4 (now {autonomy})")

    # Allowed. Remember locked files and the trunk so post can undo what slipped past.
    snapshot(call_key(event), trunk, trunk_ok=bool(merging))
    if merging and d == "ask":
        decide("ask", why)


def post(event):
    if event.get("tool_name") != "Bash":
        return
    undone = restore(call_key(event))
    if undone:
        print(json.dumps({"decision": "block", "reason": (
            "Morphogen guard: that command changed what you may not change, so it was undone: "
            + "; ".join(undone) + ". Locked means by any route; propose the change to the operator.")}))


def status_facts(event):
    cfg = dials()
    day = datetime.date.today().isoformat()
    tags = git("for-each-ref", "refs/tags/archive", "--format=%(contents)%00").stdout.split("\0")
    recs = []
    for t in tags:
        try:
            recs.append(json.loads(t.strip()))
        except json.JSONDecodeError:
            pass
    return dict(cfg=cfg, autonomy=num(cfg, "autonomy", 2), trunk=cfg.get("trunk", "main"),
                branch=current_branch() or "detached",
                calls=counter(f"calls-{session_key(event)}"), max_calls=num(cfg, "max_tool_calls_per_session"),
                trials=counter(f"trials-{day}"), max_trials=num(cfg, "max_trials_per_day"),
                archived=len(recs), passing=sum(bool(r.get("passed")) for r in recs))


LEVELS = {1: "Assist", 2: "Propose", 3: "Self-improve", 4: "Autonomous"}


def session(event):
    f = status_facts(event)
    gaps = os.path.join(ROOT, "map", "gaps.md")
    if os.path.isfile(gaps):
        age = datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(gaps))
        gap_note = f"map/gaps.md is {age.days} days old; refresh with map-self if the repo changed since"
    else:
        gap_note = "map/gaps.md does not exist yet; run map-self"
    lines = [
        f"Morphogen session. Autonomy {f['autonomy']} ({LEVELS.get(f['autonomy'], '?')}); "
        f"trunk {f['trunk']}, on branch {f['branch']}.",
        f"Budgets: {f['max_calls'] or 'unlimited'} tool calls this session; "
        f"trials today {f['trials']}/{f['max_trials'] or 'unlimited'}; "
        f"sandbox {f['cfg'].get('sandbox', 'docker')}.",
        f"Archive: {f['archived']} variants, {f['passing']} passing.",
        gap_note + ".",
    ]
    if not f["cfg"]["_purpose_set"]:
        lines.append("The intent's purpose is still the placeholder: offer the onboard skill first.")
    print("\n".join(lines))


def statusline(event):
    f = status_facts(event)
    calls = f"{f['calls']}/{f['max_calls']}" if f["max_calls"] else str(f["calls"])
    trials = f"{f['trials']}/{f['max_trials']}" if f["max_trials"] else str(f["trials"])
    print(f"morphogen · autonomy {f['autonomy']} {LEVELS.get(f['autonomy'], '')} · {f['branch']} · "
          f"calls {calls} · trials {trials} · archive {f['passing']}/{f['archived']} passing")


def main():
    global ROOT
    mode = sys.argv[1] if len(sys.argv) > 1 else "pre"
    try:
        event = json.load(sys.stdin)
    except json.JSONDecodeError:
        event = {}
    if not os.environ.get("CLAUDE_PROJECT_DIR"):
        ROOT = (event.get("workspace") or {}).get("project_dir") or event.get("cwd") or ROOT
    {"pre": pre, "post": post, "session": session, "statusline": statusline}[mode](event)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        if (sys.argv[1:2] or ["pre"])[0] == "pre":  # fail closed, to the operator
            decide("ask", f"guard error ({e.__class__.__name__}: {e}); operator decides")
        print(f"guard error: {e}", file=sys.stderr)
