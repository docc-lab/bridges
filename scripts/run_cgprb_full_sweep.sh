#!/usr/bin/env bash
# Full-corpus (NON-sampled) cgprb sweep across cpd x drop, running cells
# concurrently within a core budget, then aggregating per-cell TOPO into a
# table + grid. Each cell processes the ENTIRE day's store, so this is an
# hours-scale job (vs run_cgprb_sample_sweep.sh which samples and takes minutes).
#
#   scripts/run_cgprb_full_sweep.sh [day1|day2]
# Knobs (env): MAXCORES (default nproc), WORKERS per cell (default 8;
#   concurrent cells = MAXCORES/WORKERS), OUT (default ~/cgprb_full_sweep_<day>).
set -u
cd "$(dirname "$0")/.."

DAY=${1:-day1}
MAXCORES=${MAXCORES:-$(nproc)}
WORKERS=${WORKERS:-8}
CONC=$(( MAXCORES / WORKERS )); [ "$CONC" -lt 1 ] && CONC=1
CPDS=(2 3 4 5 6 7 8)
DROPS=(0.05 0.25 0.5 0.75 0.95)
OUT=${OUT:-$HOME/cgprb_full_sweep_${DAY}}

case "$DAY" in
  day1) STORE=/mydata/uber/day1.store; META=/mydata/uber/day1_meta
        ARC=/mydata/uber/corpus_full_unfiltered.arc;  ARCMETA=/mydata/uber/corpus_full_unfiltered/meta.bin;;
  day2) STORE=/mydata/uber/day2.store; META=/mydata/uber/day2_meta
        ARC=/mydata/uber/corpus_day2_unfiltered.arc;  ARCMETA=/mydata/uber/corpus_day2_unfiltered/meta.bin;;
  *) echo "usage: $0 [day1|day2]"; exit 2;;
esac

echo "=== build ==="
go build -tags cpsat -o bin/trace_recon_cgprb ./cmd/trace_recon || { echo "cpsat build failed (OR-Tools path?)"; exit 1; }
go build -o bin/archive_to_store ./cmd/archive_to_store || exit 1
if [ ! -f "$STORE" ]; then
  echo "=== build store $STORE (salvage default-on) ==="
  ./bin/archive_to_store --archive "$ARC" --meta "$ARCMETA" --store "$STORE" --meta-out "$META" --progress 100000 || exit 1
fi

mkdir -p "$OUT"
echo "day=$DAY FULL corpus  maxcores=$MAXCORES workers/cell=$WORKERS concurrent=$CONC -> $OUT"

echo "=== sweep (${#CPDS[@]}x${#DROPS[@]} cells, $CONC at a time, FULL corpus, heavy-first) ==="
t0=$SECONDS
# Heavy-first launch order (cost-descending). Mid-drop (0.5/0.75) x mid/high-cpd
# are the long poles per the day-2 runtimes; cpd2 is trivial. Running the heavy
# cells first keeps the box saturated and leaves a CHEAP cell as the lone tail
# (the only spot where C<concurrency leaves cores idle).
cells=()
for cpd in "${CPDS[@]}"; do
  for r in "${DROPS[@]}"; do
    case "$r" in 0.5|0.75) dw=5;; 0.25) dw=3;; 0.95) dw=2;; *) dw=1;; esac
    case "$cpd" in 2) cw=1;; 3) cw=4;; 4|5|6) cw=8;; 7) cw=6;; *) cw=5;; esac
    cells+=("$((dw * cw)) $cpd $r")
  done
done
while read -r _w cpd r; do
  out="$OUT/cpd${cpd}_r${r}.json"
  # Idempotent resume: skip a cell only if its output JSON already exists and is
  # non-empty. trace_recon writes that file last (after every trace is scored),
  # so its presence means the cell completed. A killed/OOM'd cell never wrote it
  # (a stale .err with no TOPO does NOT count as done), so it gets re-run. This
  # lets a relaunch fill in only the missing/dead cells without redoing finished
  # ones. Set FORCE=1 to ignore existing outputs and re-run everything.
  if [ -z "${FORCE:-}" ] && [ -s "$out" ]; then
    echo "skip cpd=$cpd r=$r (already done: $out)"
    continue
  fi
  (
    TRACE_RECON_TOPO=1 TRACE_RECON_CPSAT=1 ./bin/trace_recon_cgprb \
      --corpus "$META" --trace-store "$STORE" --mode cgprb \
      --checkpoint-distance "$cpd" --drop-rate "$r" --tie-policy aware --per-trace-drop-seed \
      --workers "$WORKERS" \
      -o "$OUT/cpd${cpd}_r${r}.json" 2> "$OUT/cpd${cpd}_r${r}.err"
    echo "done cpd=$cpd r=$r"
  ) &
  while [ "$(jobs -rp | wc -l)" -ge "$CONC" ]; do wait -n; done
done < <(printf '%s\n' "${cells[@]}" | sort -rn)
wait
echo "sweep done in $((SECONDS-t0))s"

# ---- aggregate ----
SUM="$OUT/summary.txt"
{
  echo "=== CGPRB FULL sweep: $DAY ==="
  printf "%-4s %-6s %-13s %-15s %-11s\n" cpd drop topo_correct topo_dropfanout sib_recall
  for cpd in "${CPDS[@]}"; do
    for r in "${DROPS[@]}"; do
      line=$(grep '^TOPO' "$OUT/cpd${cpd}_r${r}.err" 2>/dev/null)
      tc=$(sed -n 's/.*topo_correct=\([0-9.]*\)%.*/\1/p' <<<"$line")
      td=$(sed -n 's/.*topo_correct_among_them=\([0-9.]*\)%.*/\1/p' <<<"$line")
      sr=$(sed -n 's/.*sib_recall=\([0-9.]*\)%.*/\1/p' <<<"$line")
      printf "%-4s %-6s %-13s %-15s %-11s\n" "$cpd" "$r" "${tc:-FAIL}" "${td:-}" "${sr:-}"
    done
  done
  echo
  echo "=== topo_correct grid (rows=cpd, cols=drop) ==="
  printf "%-5s" "cpd\\r"; for r in "${DROPS[@]}"; do printf "%9s" "$r"; done; echo
  for cpd in "${CPDS[@]}"; do
    printf "%-5s" "$cpd"
    for r in "${DROPS[@]}"; do
      tc=$(grep '^TOPO' "$OUT/cpd${cpd}_r${r}.err" 2>/dev/null | sed -n 's/.*topo_correct=\([0-9.]*\)%.*/\1/p')
      printf "%9s" "${tc:-FAIL}"
    done
    echo
  done
} | tee "$SUM"
echo "summary -> $SUM"
