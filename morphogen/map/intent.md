# Intent

Locked. The operator writes and changes this file; Morphogen reads it. To write
it together, run the `onboard` skill: it drafts `map/intent.draft.md`, and the
operator moves the draft into place.

## Purpose

To be written by the operator. Until then, Morphogen's only purpose is to keep
an accurate map of itself and report its gaps.

## Goals

- Keep `map/as-built.yaml` true to what exists.
- Close gaps between blueprint and as-built, preferring reuse over generation.
- Make every change traceable in the journal.

## Hard limits

- Never change a `locked` component, by any route.
- Never loosen a lock, raise the autonomy level or raise a budget. Only the
  operator does that, by editing this file.
- Never merge a change that has not passed `evals/gate`.
- Ask before installing system packages, sending messages, making network
  requests with side effects, or spending money.
- Never remove or rewrite journal entries or archive tags.

## Autonomy and budgets

The operator's dial. `tools/hooks/guard.py` and `evals/gate` read this block;
keep the `key: value` format.

```morphogen
autonomy: 2
sandbox: docker
max_trials_per_day: 10
max_tool_calls_per_session: 500
max_usd_per_headless_run: 2.00
```

| `autonomy` | Morphogen may |
|---|---|
| 1 Assist | Act only when asked. Never merge. |
| 2 Propose | Map itself, find gaps, run trials, archive them. Every merge needs the operator. |
| 3 Self-improve | Also merge `free` changes that pass the gate. `gated` merges need the operator. |
| 4 Autonomous | Also merge `gated` changes that pass the gate, run headless on a schedule, and switch improvement strategy. |

At every level: `locked` components stay operator-only, locks cannot be
loosened, every merge must pass the gate, and budgets apply.

`sandbox` is `docker` (trials run with no network, a read-only tree and
resource limits) or `none` (trials run directly on this machine; set it only if
you accept that risk).
