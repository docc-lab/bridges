#!/bin/bash
# 50k day-1 traces: does the mandatory-absorption cap mask the non-deep-biased
# acceptance policies? Every policy run twice, identical but for the cap.
ROOT=/mydata/uber/policy_probe_20260924
BIN=/users/tomislav/bridges/bin/trace_sim
CORPUS=/mydata/uber/endpoint_instance_state/corpus
QIDS=$CORPUS/dee_queue_ids.bin
N=50000
CPU=1
go() { # key extra...
  local KEY=$1; shift
  GOMAXPROCS=1 taskset -c $CPU /usr/bin/time \
    -f '{"wall_seconds":%e,"user_seconds":%U,"max_rss_kib":%M,"exit_status":%x}' \
    -o "$ROOT/resources/${KEY}.json" \
    "$BIN" --corpus "$CORPUS" --mode pcrb --bagsize --workers 1 --first $N \
      --checkpoint-range 1:9 --checkpoint-seed 42 --prefix-len 8 --bloom-fp 0.0001 \
      --progress 25000 --reverse-seed 42 \
      --stream-metrics "$ROOT/sim/${KEY}.csv" \
      --checkpoint-pressure "$ROOT/sim/${KEY}.pressure.json" \
      --pressure-instance-ids "$QIDS" "$@" > /dev/null 2> "$ROOT/logs/${KEY}.log" &
  echo "  $KEY -> cpu$CPU"
  CPU=$((CPU+1))
}
go forward --reverse-policy depth_cubic --leaf-reject 0
for P in inverse_depth depth_linear depth_quadratic depth_cubic depth_quartic upstream_pressure; do
  go "${P}_stop" --reverse-policy $P --leaf-reject 1
  go "${P}_pass" --reverse-policy $P --leaf-reject 1 --reverse-pass-checkpoints
done
go prob02_stop --reverse-policy probability --reverse-probability 0.2 --leaf-reject 1
go prob02_pass --reverse-policy probability --reverse-probability 0.2 --leaf-reject 1 --reverse-pass-checkpoints
wait
echo ALL_DONE
