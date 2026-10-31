# red-queen (co-evolving challenges)

Two populations improve against each other: Morphogen's variants, and the
challenges that test them. Stops the system overfitting a fixed benchmark.

- **challenger:** writes new cases in `challenges/` that the current `main`
  fails or barely passes, aimed at the intent's purpose. Cases are data only
  (see `challenges/README.md`); they never contain scoring code.
- **solver:** any other strategy (usually hill-climb) closes the gaps the new
  cases expose.
- **after:** a challenge is kept if it is valid (the operator or a locked
  checker confirms its expected outcome is correct) and discriminating (some
  variant passes it, some fails).

The safety rule: the challenger can make the exam harder, never change the
marking. Scoring stays in `evals/` (locked). `challenges/` is gated, so new
cases need operator approval below autonomy 4.

Needs: a challenge runner that scores cases (the `challenge-runner` slot),
which does not exist yet.
