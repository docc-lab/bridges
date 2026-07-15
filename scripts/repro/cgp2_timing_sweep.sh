#!/bin/bash
export LD_LIBRARY_PATH=/users/tomislav/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib:$LD_LIBRARY_PATH
SC=/tmp/claude-36637/-users-tomislav/9c554978-de8c-42cf-bd6a-bbbe3b07797f/scratchpad; TR=$SC/trace_recon_cgp2; TDIR=/mydata/uber/bignode_state/cgp2_timing
: > "$SC/timing_run_status.txt"
declare -A DC=( [0.05]=d005 [0.25]=d025 [0.5]=d05 [0.75]=d075 [0.95]=d095 [1.0]=d10 )
flags_up="--prime-m"; flags_down="--prime-m --prime-m-bytecap"; flags_none=""
for drop in 0.05 0.25 0.5 0.75 0.95 1.0; do
  dc=${DC[$drop]}
  for day in day1 day2; do
    COR=/mydata/uber/bignode_state/${day}_unfilt_corpus
    for mode in up down none; do
      eval "f=\$flags_$mode"
      for cpd in 3 4 5 6 7 8; do
        ( $TR --corpus $COR --mode cgp2 --checkpoint-distance $cpd --prefix-len 8             --drop-rate $drop --per-trace-drop-seed $f --workers 8             --timing "$TDIR/timing_${day}_${mode}_c${cpd}_${dc}.csv" -o /dev/null             > "$SC/tim_${day}_${mode}_c${cpd}_${dc}.txt" 2>&1
          echo "$drop $day $mode cpd=$cpd DONE $(date +%H:%M:%S)" >> "$SC/timing_run_status.txt" ) &
      done
      wait
      echo "=== round: $drop $day $mode done $(date +%H:%M:%S) ===" >> "$SC/timing_run_status.txt"
    done
  done
  echo "DROPDONE $drop $(date +%H:%M:%S)" >> "$SC/timing_run_status.txt"
done
echo ALLDONE >> "$SC/timing_run_status.txt"
