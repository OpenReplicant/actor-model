# Challenges

Test cases for the intent's purpose, written by the operator or by the
`red-queen` strategy. Gated. Data only: one YAML file per case.

```yaml
id: short-name
purpose: which goal in map/intent.md this tests
input: what Morphogen is given
expect: how a correct result is recognised (checked by the challenge-runner)
source: operator | red-queen
```

Scoring code never lives here. The `challenge-runner` slot (empty) will turn
these into a KPI that `evals/run` can read.
