#!/bin/bash
# Day-2 simulation, CPD 1:9 / depth_cubic. One job per (bridge, arm), one physical core each.
ROOT=/home/ubuntu/sim_day2
BIN=$ROOT/bin_trace_sim
CORPUS=/mydata/uber/day2_unfilt_corpus
QIDS=/mydata/uber/day2_sidecar/dee_queue_ids.bin
CPU=1
for MODE in pcrb cgprb sb3; do
  for ARM in forward reverse; do
    [ "$ARM" = reverse ] && Q=1 || Q=0
    KEY="${MODE}_${ARM}"
    EXTRA=()
    [ "$MODE" = sb3 ] && EXTRA=(--fp-bits 64 --lehmer-ee --dee-dequeue-one --dee-queue-ids "$QIDS")
    GOMAXPROCS=1 taskset -c $CPU /usr/bin/time \
      -f '{"wall_seconds":%e,"user_seconds":%U,"system_seconds":%S,"max_rss_kib":%M,"exit_status":%x}' \
      -o "$ROOT/resources/${KEY}.resources.json" \
      "$BIN" --corpus "$CORPUS" --mode "$MODE" --bagsize --workers 1 \
        --checkpoint-range 1:9 --checkpoint-seed 42 --prefix-len 8 --bloom-fp 0.0001 \
        --progress 10000 \
        --reverse-policy depth_cubic --leaf-reject $Q --reverse-seed 42 \
        --stream-metrics "$ROOT/sim/${KEY}.csv" \
        --size-histograms "$ROOT/sim/${KEY}.hist.json" \
        --checkpoint-pressure "$ROOT/sim/${KEY}.pressure.json" \
        --pressure-instance-ids "$QIDS" \
        "${EXTRA[@]}" > /dev/null 2> "$ROOT/logs/${KEY}.log" &
    echo "launched $KEY on cpu$CPU pid=$!"
    CPU=$((CPU+1))
  done
done
wait
echo "ALL_SIM_JOBS_DONE"
