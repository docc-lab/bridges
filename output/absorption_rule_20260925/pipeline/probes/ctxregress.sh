#!/bin/bash
# Regression: the two-byte context must leave every range that already fit one
# byte byte-for-byte unchanged. Same flags and seeds as relbin_probe_20260924,
# whose outputs were produced before the change; the CSVs are diffed directly.
ROOT=/mydata/uber/ctxregress_20260924
BIN=/users/tomislav/bridges/bin/trace_sim
CORPUS=/mydata/uber/endpoint_instance_state/corpus
CPU=1
for K in depth_cubic_stop upstream_pressure_pass; do
  P=${K%_*}; A=${K##*_}
  EXTRA=""; [ "$A" = pass ] && EXTRA="--reverse-pass-checkpoints"
  GOMAXPROCS=1 taskset -c $CPU "$BIN" --corpus "$CORPUS" --mode pcrb --bagsize \
    --workers 1 --first 50000 --checkpoint-range 1:9 --checkpoint-seed 42 \
    --prefix-len 8 --bloom-fp 0.0001 --progress 25000 --reverse-seed 42 \
    --stream-metrics "$ROOT/sim/${K}.csv" \
    --checkpoint-pressure "$ROOT/sim/${K}.pressure.json" \
    --pressure-instance-ids "$CORPUS/dee_queue_ids.bin" \
    --reverse-policy "$P" --leaf-reject 1 $EXTRA > /dev/null 2> "$ROOT/logs/${K}.log" &
  CPU=$((CPU+1))
done
wait
echo ALL_DONE
