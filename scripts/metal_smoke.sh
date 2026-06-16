#!/usr/bin/env bash
# Compile + smoke-test cgprb on the metal box. Run from the repo root.
# (If OR-Tools isn't at the path baked in recon/cpsat_cgo.go, repoint it first:
#  sed -i "s#/users/tomislav#$HOME#g" recon/cpsat_cgo.go recon/fullsat_cgo.go )
set -u

# --- compile (this is the OR-Tools link gate) ---
go build -tags cpsat -o bin/trace_recon_cgprb ./cmd/trace_recon && echo BUILD_OK
go build -o bin/archive_to_store ./cmd/archive_to_store && echo TOOLS_OK

# --- build the day-1 store once (bounded memory, ~mins) ---
./bin/archive_to_store \
  --archive /mydata/uber/corpus_full_unfiltered.arc \
  --meta    /mydata/uber/corpus_full_unfiltered/meta.bin \
  --store   /mydata/uber/day1.store \
  --meta-out /mydata/uber/day1_meta \
  --progress 100000

# --- smoke: one capped cell, expect a TOPO line in seconds ---
TRACE_RECON_TOPO=1 TRACE_RECON_CPSAT=1 ./bin/trace_recon_cgprb \
  --corpus /mydata/uber/day1_meta --trace-store /mydata/uber/day1.store \
  --mode cgprb --checkpoint-distance 6 --drop-rate 0.75 --tie-policy aware --per-trace-drop-seed \
  --trace-count 20000 --workers 32 -o /tmp/smoke.json 2>&1 | grep -E '^TOPO|error|panic|cannot'
