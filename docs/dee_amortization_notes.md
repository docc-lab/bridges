# Delayed end events (DEE): amortization notes

Informal design notes (filed for later). Not a spec.

## Problem

In S-bridge, deferred work can accumulate as **large `dee_bytes`** blocks. When a span **starts** on a service, it may **drain** the per-service DEE queue and merge that payload into inline baggage. A **single huge pickup** then propagates **downstream across many descendants** until a **checkpoint** clears inline state. That pattern inflates **per-call baggage** and especially **tails** (worst-case / high percentiles).

## Idea

**Amortize** DEE: instead of delaying everything until one consumer picks up one giant blob, **split deferred work into smaller ordered chunks** and **flush progressively** so no single `on_start` has to absorb the entire backlog at once.

## Why this might shrink reported sizes

- Metrics that include calls dominated by one large DEE pickup should move **down** if chunk maxima are bounded.
- **Means / high percentiles** often improve when DEE tails dominate; **medians** may move less if most traces rarely hit large DEE.

## Possible strategies

1. **Byte cap / chunking** — When pending DEE would exceed \(K\) bytes, emit a **prefix chunk** to the next logical handoff (ordering preserved). Remaining bytes stay queued or move to the next flush point.
2. **Count cap** — Same, but split after \(m\) deferred **start ordinals** (end-events) rather than raw bytes.
3. **Checkpoint-aligned flush** — On S-bridge checkpoint spans, also flush a **bounded prefix** of pending DEE so baggage does not survive as one lump across long subtrees.
4. **Schedule / ordering budget** — In a **simulator**, event order is fixed; inserting synthetic flush points changes the experiment and must be labeled as such.

## Tradeoffs

- **Ordering**: chunks must concatenate to the **same total sequence** as today; reordering breaks reconstruction.
- **Overhead**: more handoffs / more triples / more queue ops can add **cost elsewhere** (not only baggage).
- **Scope**: shrinking DEE does not cap **inline** ordinal / other payload growth; expectations should stay scoped.

## Sensible first prototype

**Byte cap + ordered chunking** is usually the easiest to reason about and to correlate with observed large DEE spikes (e.g. logs or size histograms).
