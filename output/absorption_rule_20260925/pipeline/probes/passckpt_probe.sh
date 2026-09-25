#!/bin/bash
# 50k day-1 traces: does letting a truss pass a scheduled checkpoint move load?
# Only --reverse-pass-checkpoints differs between the two reverse arms.
ROOT=/mydata/uber/passckpt_probe_20260924
BIN=/users/tomislav/bridges/bin/trace_sim
CORPUS=/mydata/uber/endpoint_instance_state/corpus
QIDS=$CORPUS/dee_queue_ids.bin
N=50000
run() { # name cpu extra...
  local KEY=$1 CPU=$2; shift 2
  GOMAXPROCS=1 taskset -c $CPU /usr/bin/time \
    -f '{"wall_seconds":%e,"user_seconds":%U,"system_seconds":%S,"max_rss_kib":%M,"exit_status":%x}' \
    -o "$ROOT/resources/${KEY}.json" \
    "$BIN" --corpus "$CORPUS" --mode pcrb --bagsize --workers 1 --first $N \
      --checkpoint-range 1:9 --checkpoint-seed 42 --prefix-len 8 --bloom-fp 0.0001 \
      --progress 10000 --reverse-seed 42 \
      --stream-metrics "$ROOT/sim/${KEY}.csv" \
      --size-histograms "$ROOT/sim/${KEY}.hist.json" \
      --checkpoint-pressure "$ROOT/sim/${KEY}.pressure.json" \
      --pressure-instance-ids "$QIDS" "$@" > /dev/null 2> "$ROOT/logs/${KEY}.log" &
  echo "launched $KEY on cpu$CPU pid=$!"
}
run forward       1 --reverse-policy depth_cubic --leaf-reject 0
run rev_stop      2 --reverse-policy depth_cubic --leaf-reject 1
run rev_pass      3 --reverse-policy depth_cubic --leaf-reject 1 --reverse-pass-checkpoints
wait
echo ALL_DONE
