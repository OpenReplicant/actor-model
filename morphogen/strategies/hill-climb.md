# hill-climb (default)

Greedy, one lineage. Cheap and easy to follow.

- **parent:** the tip of `main`.
- **candidates:** one at a time, in sourcing order (reuse, configure, compose,
  generate). Try the next only if the previous fails the gate.
- **after:** adopt the first candidate that passes; the rest stay archived.

Good when there is one clear gap and a working system to protect. Weak when
every single step looks worse even though a sequence of steps would be better
(a local optimum); try `archive-search` then.
