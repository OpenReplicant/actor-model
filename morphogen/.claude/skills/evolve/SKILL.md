---
name: evolve
description: Close one gap from map/gaps.md using the active improvement strategy - source candidates in order of reuse, configure, compose, generate; trial each on a branch in the sandbox; score it with evals/gate, which archives every variant; and adopt according to lock level and autonomy. Use when the operator asks for a new capability, when a KPI misses its target, or when working through the gap list.
---

# evolve

Close one gap at a time. Never touch a `locked` component.

## 0. Read the dials

- Autonomy level and budgets: the ` ```morphogen ` block in `map/intent.md`.
  At autonomy 1, act only on an explicit request.
- Active strategy: component `improvement-strategy` in `map/blueprint.yaml`;
  read its file in `strategies/` and follow its `parent`, `candidates` and
  `after` rules in the steps below.

## 1. Frame the gap

- Name the component or slot it belongs to. If none fits, the fix starts as a
  blueprint change (gated): propose the new component with its `provides`,
  `kpis` and `lock`.
- State the capability needed and the KPI that will show it is closed. A KPI
  needs a runnable `measured_by`: `eval:<check>` for a built-in check in
  `evals/run`, or `cmd:<shell>` that prints a number and runs in the sandbox.
  If no KPI can be measured, agree one with the operator first.
- Check the lock level. `locked`: stop and propose in chat.

## 2. Source, in this order

1. **Reuse:** search the catalogs in `catalogs.yaml` for an existing part that
   provides the capability. Check maintenance, licence, adoption and docs.
2. **Configure:** change settings of a part already in the map.
3. **Compose:** combine existing parts, for example a cascade, a wrapper or an
   MCP server in front of a tool.
4. **Generate:** write new code, a skill or a prompt. Only after the first three
   fail; record why each failed.

Collect as many candidates as the strategy asks for.

## 3. Trial

- Branch from the strategy's parent: `git checkout -b evolve/<component>-<short-gap> <parent>`.
- Apply the candidate. Pin external parts to exact versions and hashes.
- Run anything the candidate adds or changes through `tools/sandbox/run`, never
  directly. Installs and network use need the operator (the guard asks).
- Commit, then score: `evals/gate --base <parent> --strategy <name> --gap "<gap>"`.
  The gate judges with the base's evals, so you cannot change how you are
  scored. Every run is archived as a tag, pass or fail.

## 4. Adopt or discard

You cannot commit on the trunk (`trunk:` in the intent block, usually `main`);
it changes only by merging a commit whose gate passed against the trunk **as it
is now**. If the trunk moved since, merge or rebase the trunk into your branch
and run the gate again. On your own branches, commit, merge and rebase freely.

| | autonomy 1-2 | autonomy 3 | autonomy 4 |
|---|---|---|---|
| change touches only `free` paths | operator approves | merge | merge |
| change touches `gated` paths | operator approves | operator approves | merge |
| change touches `locked` paths | refused | refused | refused |

When asking, show the gap, the candidate, its rung, and the gate table.
A failed candidate is not deleted: its branch may go, its archive tag stays.

## 5. Record

- Update `map/blueprint.yaml` (on the same branch, so it shares the gate and the
  approval) to name the adopted part, its source, version and the
  `alternatives` you rejected.
- Append the journal entry described in `CLAUDE.md`, including a
  `**Rung:** reuse|configure|compose|generate` line and the archive tag.
- Run `map-self` so as-built and gaps reflect the change. (Both files are
  generated and not committed.)
