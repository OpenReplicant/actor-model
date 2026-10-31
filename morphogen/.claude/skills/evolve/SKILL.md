---
name: evolve
description: Close one gap from map/gaps.md by sourcing a part in order of reuse, configure, compose, generate; trialling it on a branch against KPIs; and adopting it according to its lock level. Use when the operator asks for a new capability, when a KPI misses its target, or when working through the gap list.
---

# evolve

Close one gap at a time. Never touch a `locked` component.

## 1. Frame the gap

- Name the component or slot it belongs to. If none fits, the fix starts as a
  blueprint change (gated): propose the new component with its `provides`,
  `kpis` and `lock`.
- State the capability needed and the KPI that will show it is closed. If no
  KPI can be measured, say so and agree one with the operator first.
- Check the lock level. `locked`: stop and propose in chat. `gated`: you will
  need approval before merging. `free`: you may merge if KPIs do not regress.

## 2. Source, in this order

1. **Reuse:** search the catalogs in `catalogs.yaml` for an existing part that
   provides the capability. Check maintenance, licence, adoption and docs.
2. **Configure:** change settings of a part already in the map.
3. **Compose:** combine existing parts, for example a cascade, a wrapper or an
   MCP server in front of a tool.
4. **Generate:** write new code, a skill or a prompt. Only after the first three
   fail; record why each failed.

Collect two or three candidates when possible, not just the first match.

## 3. Trial

- Create a branch named `evolve/<component>-<short-gap>`.
- Measure the KPI before the change.
- Apply the candidate. Pin external parts to exact versions and hashes.
- Measure the KPI after, plus every other KPI the change could affect.
- Ask the operator before anything listed under hard limits in
  `map/intent.md`.

## 4. Adopt or discard

- **Regressed or missed target:** discard the branch, keep the result in the
  journal, try the next candidate.
- **Met target, `free`:** merge.
- **Met target, `gated`:** show the operator the gap, the chosen part, the
  rung, and KPIs before and after; merge only when they approve.

## 5. Record

- Update `map/blueprint.yaml` (on the same branch, so it shares the approval) to
  name the adopted part, its source, version and `alternatives` you rejected.
- Append the journal entry described in `CLAUDE.md`.
- Run `map-self` so as-built and gaps reflect the change.
