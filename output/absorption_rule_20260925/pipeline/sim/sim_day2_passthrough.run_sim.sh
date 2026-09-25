#!/bin/bash
# Day-2 full-corpus bagsize, reverse arm, WITH --reverse-pass-checkpoints.
# Partner to sim_day2_cpd19_depth_cubic (identical but for that one flag), so
# every delta is attributable. The forward arm is untouched by the flag and is
# deliberately not re-run.
ROOT=/mydata/uber/sim_day2_passthrough_20260924
BIN=$ROOT/bin_trace_sim
CORPUS=/mydata/uber/bignode_state/day2_unfilt_corpus
QIDS=$CORPUS/dee_queue_ids.bin
CPU=1
for MODE in pcrb cgprb sb3; do
  KEY="${MODE}_reverse_pass"
  EXTRA=()
  [ "$MODE" = sb3 ] && EXTRA=(--fp-bits 64 --lehmer-ee --dee-dequeue-one --dee-queue-ids "$QIDS")
  GOMAXPROCS=1 taskset -c $CPU /usr/bin/time \
    -f '{"wall_seconds":%e,"user_seconds":%U,"system_seconds":%S,"max_rss_kib":%M,"exit_status":%x}' \
    -o "$ROOT/resources/${KEY}.resources.json" \
    "$BIN" --corpus "$CORPUS" --mode "$MODE" --bagsize --workers 1 \
      --checkpoint-range 1:9 --checkpoint-seed 42 --prefix-len 8 --bloom-fp 0.0001 \
      --progress 25000 \
      --reverse-policy depth_cubic --leaf-reject 1 --reverse-seed 42 \
      --reverse-pass-checkpoints \
      --stream-metrics "$ROOT/sim/${KEY}.csv" \
      --size-histograms "$ROOT/sim/${KEY}.hist.json" \
      --checkpoint-pressure "$ROOT/sim/${KEY}.pressure.json" \
      --pressure-instance-ids "$QIDS" \
      "${EXTRA[@]}" > /dev/null 2> "$ROOT/logs/${KEY}.log" &
  echo "launched $KEY on cpu$CPU pid=$!"
  CPU=$((CPU+1))
done
wait
echo ALL_SIM_JOBS_DONE
