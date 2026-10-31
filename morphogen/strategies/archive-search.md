# archive-search (Darwin Gödel Machine style)

Open-ended search over the archive instead of one lineage. After the Darwin
Gödel Machine (Zhang et al., 2025): keep every variant, branch from promising
and under-explored ones, not only the best.

- **parent:** pick from `tools/archive/list --json`, weighting each variant by
  its scores and by how few children it already has (count archive records whose
  `base` is that commit). Failed variants are eligible: a step that regressed may
  lead somewhere a passing one cannot.
- **candidates:** two to four per round, each on its own branch from the parent,
  each run through `evals/gate --base <parent> --strategy archive-search`.
- **after:** variants are archived by the gate. A variant is *adopted* (merged
  to `main`) only when it passes the gate against `main` itself, not just
  against its parent.

Needs: a trial budget of at least ten a day, and KPIs that measure real
capability, not just map hygiene. Without good evals this searches noise.
Costs several times more than hill-climb per gap closed.
