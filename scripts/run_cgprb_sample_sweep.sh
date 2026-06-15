#!/usr/bin/env bash
# Sweep cgprb over a RANDOM SAMPLE of traces, across cpd x drop, running many
# cells at once within a core budget, then aggregate per-cell TOPO into a table.
#
#   scripts/run_cgprb_sample_sweep.sh [day1|day2]
# Knobs (env):
#   MAXCORES=<n>    total core budget (default: nproc)
#   WORKERS=<n>     workers per cell (default 8; CP-SAT spawns internal threads,
#                   so keep modest — concurrent cells = MAXCORES/WORKERS)
#   SAMPLE=<n>      random traces per cell (default 20000); same SAMPLE_SEED =>
#                   identical sample across every cell
#   SAMPLE_SEED=<n> sample selection seed (default 1)
#   OUT=<dir>       results dir (default ~/cgprb_sample_sweep_<day>)
set -u
cd "$(dirname "$0")/.."

DAY=${1:-day1}
SAMPLE=${SAMPLE:-20000}
SAMPLE_SEED=${SAMPLE_SEED:-1}
MAXCORES=${MAXCORES:-$(nproc)}
WORKERS=${WORKERS:-8}
CONC=$(( MAXCORES / WORKERS )); [ "$CONC" -lt 1 ] && CONC=1
CPDS=(2 3 4 5 6 7 8)
DROPS=(0.05 0.25 0.5 0.75 0.95)
OUT=${OUT:-$HOME/cgprb_sample_sweep_${DAY}}

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

# Build the (salvaged) store once if it isn't there yet.
if [ ! -f "$STORE" ]; then
  echo "=== build store $STORE (salvage default-on) ==="
  ./bin/archive_to_store --archive "$ARC" --meta "$ARCMETA" --store "$STORE" --meta-out "$META" --progress 100000 || exit 1
fi

mkdir -p "$OUT"
echo "day=$DAY sample=$SAMPLE seed=$SAMPLE_SEED maxcores=$MAXCORES workers/cell=$WORKERS concurrent=$CONC -> $OUT"

echo "=== sweep (${#CPDS[@]}x${#DROPS[@]} cells, $CONC at a time) ==="
t0=$SECONDS
for cpd in "${CPDS[@]}"; do
  for r in "${DROPS[@]}"; do
    (
      TRACE_RECON_TOPO=1 TRACE_RECON_CPSAT=1 ./bin/trace_recon_cgprb \
        --corpus "$META" --trace-store "$STORE" --mode cgprb \
        --checkpoint-distance "$cpd" --drop-rate "$r" --tie-policy aware --per-trace-drop-seed \
        --sample "$SAMPLE" --sample-seed "$SAMPLE_SEED" --workers "$WORKERS" \
        -o "$OUT/cpd${cpd}_r${r}.json" 2> "$OUT/cpd${cpd}_r${r}.err"
      echo "done cpd=$cpd r=$r"
    ) &
    while [ "$(jobs -rp | wc -l)" -ge "$CONC" ]; do wait -n; done
  done
done
wait
echo "sweep done in $((SECONDS-t0))s"

# ---- aggregate ----
SUM="$OUT/summary.txt"
{
  echo "=== CGPRB sample sweep: $DAY, sample=$SAMPLE seed=$SAMPLE_SEED ==="
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
