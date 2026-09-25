#!/bin/bash
# 50k day-1 traces across a CHECKPOINT-DENSITY sweep, to test whether mandatory
# absorption stops being the cheap option as scheduled checkpoints get rare.
#
#   1:9   E[D]= 5.0   the density every other probe uses (already run: relbin)
#   1:16  E[D]= 8.5   the sparsest range expressible with min=1; the wire format
#                     packs assigned distance and remaining TTL into one byte, so
#                     bits(max-1)+bits(max-min) <= 8 caps max at 16 for min=1
#   14:21 E[D]=17.5   reaches distance 21 by raising the floor instead
#
# Two policies, one deep-biased and one shallow-biased, each stop vs pass.
ROOT=/mydata/uber/cpdsparse_probe_20260924
BIN=/users/tomislav/bridges/bin/trace_sim
CORPUS=/mydata/uber/endpoint_instance_state/corpus
QIDS=$CORPUS/dee_queue_ids.bin
N=50000
CPU=1
go() { # key range extra...
  local KEY=$1 RANGE=$2; shift 2
  GOMAXPROCS=1 taskset -c $CPU /usr/bin/time \
    -f '{"wall_seconds":%e,"user_seconds":%U,"max_rss_kib":%M,"exit_status":%x}' \
    -o "$ROOT/resources/${KEY}.json" \
    "$BIN" --corpus "$CORPUS" --mode pcrb --bagsize --workers 1 --first $N \
      --checkpoint-range "$RANGE" --checkpoint-seed 42 --prefix-len 8 --bloom-fp 0.0001 \
      --progress 25000 --reverse-seed 42 \
      --stream-metrics "$ROOT/sim/${KEY}.csv" \
      --checkpoint-pressure "$ROOT/sim/${KEY}.pressure.json" \
      --pressure-instance-ids "$QIDS" "$@" > /dev/null 2> "$ROOT/logs/${KEY}.log" &
  echo "  $KEY ($RANGE) -> cpu$CPU"
  CPU=$((CPU+1))
}
for R in 1:16 14:21; do
  TAG=${R/:/_}
  for P in depth_cubic upstream_pressure; do
    go "${P}_${TAG}_stop" "$R" --reverse-policy $P --leaf-reject 1
    go "${P}_${TAG}_pass" "$R" --reverse-policy $P --leaf-reject 1 --reverse-pass-checkpoints
  done
done
wait
echo ALL_DONE
