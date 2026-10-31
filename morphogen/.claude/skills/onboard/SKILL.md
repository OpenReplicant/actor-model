---
name: onboard
description: Interview the operator and draft map/intent.md with them (purpose, goals, hard limits, autonomy level, budgets, sandbox), then propose the first blueprint changes the purpose needs. Use on first start when the intent's purpose is still the placeholder, or when the operator wants to change what Morphogen is for or how much freedom it has.
---

# onboard

Turn what the operator wants into an intent Morphogen can act on. You cannot
edit `map/intent.md` (it is locked); you write a draft and the operator moves it
into place.

## 1. Map first

Run `map-self` if `map/as-built.yaml` is missing, so you can describe what
exists today in one or two lines.

## 2. Interview

Ask a few questions at a time, in plain words. Offer examples; accept short
answers. Cover:

1. **Purpose:** what should Morphogen do for them? Who or what does it serve?
   Push gently for something checkable: "keep my inbox triaged by 9am" beats
   "help with email".
2. **Where work comes from:** they ask in chat, files in a folder, a calendar,
   an issue tracker, sensors. Each source is a candidate for the `observers` slot.
3. **What good looks like:** two or three outcomes you could measure. These
   become KPIs and, later, cases in `challenges/`.
4. **Hard limits:** anything Morphogen must never do or touch beyond the
   defaults (accounts, people, money, data that must not leave the machine).
5. **Autonomy:** explain the four levels in the intent's table and recommend
   2 (Propose) to start. Say plainly what 4 means: it merges its own gated
   changes and runs unattended within budget. Locked parts, the gate, the
   sandbox and the journal stay fixed at every level.
6. **Budgets:** trials per day, tool calls per session, dollars per headless
   run. Offer the current values as defaults.
7. **Sandbox:** keep `docker` unless they cannot run Docker; if they choose
   `none`, say what that exposes (trials run on their machine with network).

## 3. Draft

Write `map/intent.draft.md` with the same sections and the same
` ```morphogen ` block format as `map/intent.md`. Keep the default hard limits
and add theirs. Show them the diff (`diff map/intent.md map/intent.draft.md`).

## 4. Hand over

Tell the operator to review the draft and, if they agree, run it themselves:

```
! mv map/intent.draft.md map/intent.md && git add map/intent.md && git commit -m "Set intent"
```

Do not run that yourself; the guard refuses it, and the decision is theirs.

## 5. Propose the first gaps

With the new intent in place, run `map-self`. The purpose usually implies
components the blueprint lacks (an observer for their work source, a KPI for
each outcome, challenge cases). Propose them as one gated blueprint change via
`evolve`, sourcing rung by rung. Do not build anything else until they approve.
