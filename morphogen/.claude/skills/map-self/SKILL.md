---
name: map-self
description: Build or refresh Morphogen's map of itself (map/as-built.yaml) and the gap list (map/gaps.md) by inventorying what actually exists and comparing it with map/blueprint.yaml. Use at the start of a session when the map is missing or stale, after any change, or when the operator asks what the system is made of.
---

# map-self

Produce an accurate, deterministic map of what exists, then compare it with
what is intended.

## 1. Use the mapper script if there is one

If `tools/map/` contains a mapper script, run it and go to step 4. A script is
preferred because the same repo state must always give the same map.

## 2. Otherwise, inventory by hand

Cover at least:

- **Files:** every tracked file (`git ls-files`), grouped by component path.
- **Claude Code configuration:** skills in `.claude/skills/`, hooks and
  permissions in `.claude/settings.json`, subagents in `.claude/agents/`, MCP
  servers in `.mcp.json`.
- **Dependencies:** any manifest (`package.json`, `pyproject.toml`,
  `requirements.txt`, `go.mod`, `Cargo.toml`, `Dockerfile`), with pinned
  versions.
- **Couplings:** which components read, call or configure which others.
- **Tools available in the environment** that components rely on (for example
  `git`, `python3`, `docker`), with versions.
- **Sandbox and archive:** whether `tools/sandbox/run` works (`tools/sandbox/run
  -- true`), and the archive size (`tools/archive/list | wc -l`).

## 3. Make the next map cheaper

On the first hand inventory, propose a mapper script in `tools/map/` that
produces the same output. This is generated code, so it goes through `evolve`
as a gated change. Until it is approved, map by hand.

## 4. Write map/as-built.yaml

Use the blueprint's structure: one entry per component found, with `path`,
`provides` and `consumes` where they can be observed, `version` for external
parts, and measured `kpis`: run `evals/run --json` for the built-in checks and
the `cmd:` KPIs, and copy the values. List files
that belong to no component under `unmapped:`. Add a `generated:` field with
the date and the commit hash.

## 5. Write map/gaps.md

Compare as-built with the blueprint and `map/intent.md`. List:

- **Missing:** components or capabilities intended but not present.
- **Under target:** KPIs that miss their target, with measured value and target.
- **Unintended:** components, couplings or files present but not in the
  blueprint.
- **Drift:** components whose path, version or interface differs from the
  blueprint.
- **Lock coverage:** locked paths that `.claude/settings.json` does not deny.
- **Unmeasured:** KPIs whose `measured_by` is prose rather than `eval:` or
  `cmd:`; each is a gap, because the gate cannot protect it.

Order by impact on the intent. Keep each gap to one line plus its evidence.

## 6. Report

Tell the operator the component count, the top gaps, and anything that needs
their decision. Do not start fixing gaps inside this skill.
