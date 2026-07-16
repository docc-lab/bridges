#!/bin/bash
# Multi-drop reconstruction sweep. One pass per (recon, day, prime, cpd)
# reconstructs ALL drop rates in $RATES from a single trace decode (the
# --drop-rates path), so the corpus is read/emitted once per pass instead of
# once per rate. Uses the per-trace store (parallel, no serial event streaming).
#
# Outputs under $OUT_DIR:
#   <recon>_<day>_<prime>_c<cpd>.log            -- accuracy (CGP2[dc]/PB2[dc]
#                                                  correctExclEmpty per rate) + PROGRESS
#   <recon>_timing/timing_<day>_<prime>_c<cpd>_<dc>.csv  -- per-trace recon_ns per rate
#   sweep_status.txt                            -- WAVE/CFG START/DONE markers
#
# Prereq: build with the cpsat tag and export the OR-Tools lib on LD_LIBRARY_PATH:
#   export LD_LIBRARY_PATH=/path/to/or-tools_.../lib:$LD_LIBRARY_PATH
#   go build -tags cpsat -o trace_recon ./cmd/trace_recon
#
# Concurrency = (#CPDS) x WORKERS threads. Keep it <= (cores - a few) so every
# in-flight reconstruction gets its own core and recon_ns is uncontended (clean
# timing). e.g. 192-core box: WORKERS=28 x 6 cpds = 168.
#
# Env overrides (defaults in parens):
#   BIN(./trace_recon) DATA_DIR(data) META_DIR(meta) OUT_DIR(out) WORKERS(28)
#   RATES(0.05,0.25,0.5,0.75,0.95,1.0) RECONS("cgp2 pb2") DAYS("day1 day2")
#   PRIMES("up down none") CPDS("3 4 5 6 7 8")
# DATA_DIR holds <day>.store; META_DIR holds <day>_unfilt_corpus/meta.bin.
set -u
BIN=${BIN:-./trace_recon}
DATA_DIR=${DATA_DIR:-data}
META_DIR=${META_DIR:-meta}
OUT_DIR=${OUT_DIR:-out}
WORKERS=${WORKERS:-28}
RATES=${RATES:-0.05,0.25,0.5,0.75,0.95,1.0}
RECONS=${RECONS:-"cgp2 pb2"}
DAYS=${DAYS:-"day1 day2"}
PRIMES=${PRIMES:-"up down none"}
CPDS=${CPDS:-"3 4 5 6 7 8"}
declare -A PF=( [up]="--prime-m" [down]="--prime-m --prime-m-bytecap" [none]="" )
STATUS=$OUT_DIR/sweep_status.txt
mkdir -p "$OUT_DIR"; : > "$STATUS"
for recon in $RECONS; do
  TDIR=$OUT_DIR/${recon}_timing; mkdir -p "$TDIR"
  for day in $DAYS; do
    for prime in $PRIMES; do
      echo "WAVE $recon $day $prime START $(date +%H:%M:%S)" >> "$STATUS"
      for cpd in $CPDS; do
        ( $BIN --trace-store "$DATA_DIR/${day}.store" --corpus "$META_DIR/${day}_unfilt_corpus" \
            --mode "$recon" --checkpoint-distance "$cpd" --prefix-len 8 --per-trace-drop-seed \
            --drop-rates "$RATES" ${PF[$prime]} --workers "$WORKERS" \
            --timing "$TDIR/timing_${day}_${prime}_c${cpd}_{dc}.csv" -o /dev/null \
            > "$OUT_DIR/${recon}_${day}_${prime}_c${cpd}.log" 2>&1
          echo "CFG $recon $day $prime c$cpd DONE $(date +%H:%M:%S)" >> "$STATUS" ) &
      done
      wait
      echo "WAVE $recon $day $prime DONE $(date +%H:%M:%S)" >> "$STATUS"
    done
  done
done
echo ALLDONE >> "$STATUS"
