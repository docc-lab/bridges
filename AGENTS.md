# Project context

This project concerns distributed tracing: span loss, ancestry propagation,
trace reconstruction, call graphs, and event ordering. The user explicitly
requires that analyses stay in this domain and never frame the work as
cybersecurity or threat detection.

Uber input data lives under `/mydata/uber` and its subdirectories. Keep generated
validation artifacts in this checkout's `.local/` directory. Local setup and
the current PB0/CGP0/SB3 implementation map are documented in
`docs/LOCAL_GUIDE.md`; activate the installed tools with `source env.local.sh`.

# Evaluation requirements

This evaluation supports a systems-conference paper. Implement the requested
simulation behavior and verify its emitted evidence and byte accounting before
presenting results. Do not substitute an assumed transport/export schema for
missing implementation details or describe its sizes as measured SDK costs.
Analytical models requested for analysis are separate from simulator measurements.
Use this simulator's native binary payload conventions for reverse propagation;
precise compatibility with Blueprint's transport envelope is not required.

State units, denominators, included bytes, and workload/configuration provenance.
Show absolute costs when comparing bridge types; a normalized dispersion measure
such as CV must never be presented as evidence that one bridge uses fewer bytes.
Preserve original data and make corrections traceable. Report incomplete work
and unverified results explicitly, including reverse reconstruction if only its
evidence decoder has been implemented.

Native evaluation runs may operate in parallel, but assign at most one trace
computation to each physical core; do not assign concurrent runs to SMT siblings.
Never run `git remote -v`.
