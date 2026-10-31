# Improvement strategies

A strategy decides **which variant to start from and which candidates to
try**. It never decides what is kept: that is `evals/gate`, locked, the same
for every strategy. Strategies compete only on the quality of what they
propose.

Interface, followed by the `evolve` skill:

- `parent(archive) -> commit`: where the next trial branches from.
- `candidates(gap, parent) -> 1..n changes`: what to try, each on its own branch.
- `after(results)`: what to record or keep for next time.

The active strategy is named in `map/blueprint.yaml` (component
`improvement-strategy`). Switching is a gated blueprint change; at autonomy 4
Morphogen may switch itself if the switch passes the gate. More strategies are
listed under `strategies:` in `catalogs.yaml`; adopting one means writing its
file here through `evolve`, like any other part.
