#!/usr/bin/env bash
# Baggage/payload overhead sweep with the simulator calculator (trace_sim
# --bagsize) across bridge MODES x cpd, on a RANDOM SAMPLE of traces, then
# aggregate the per-cell overhead into a table. Overhead is emission-only, so
# there is NO drop-rate axis here.
#
#   scripts/run_bagsize_sample_sweep.sh [corpus_dir]   (default /mydata/uber/corpus_full)
# Knobs (env):
#   SAMPLE=<n>      random traces per cell (default 20000; match the recon sweep's
#                  --sample/--sample-seed for overhead-vs-accuracy on the same traces)
#   SAMPLE_SEED=<n> (default 1)
#   MAXCORES=<n>   concurrent cells (trace_sim is single-threaded, ~1 core/cell; default nproc)
#   MODES="..."    default "vanilla pb cgpb sbridge"
#   OUT=<dir>      default ~/bagsize_sample_sweep
#
# NOTE: needs an events.bin corpus (--corpus). S-bridge's baggage uses a
# cross-trace DEE queue, so a SAMPLE under-amortizes sbridge baggage (it sees
# fewer traces per service); pb/cgpb/vanilla are per-trace and unbiased. For a
# faithful sbridge baggage number, run the full corpus (SAMPLE=0).
set -u
cd "$(dirname "$0")/.."

CORPUS=${1:-/mydata/uber/corpus_full}
SAMPLE=${SAMPLE:-20000}
SAMPLE_SEED=${SAMPLE_SEED:-1}
PROGRESS=${PROGRESS:-50000}   # per-cell: print PROGRESS traces=N to the cell's .err every N traces (0 = silent)
MAXCORES=${MAXCORES:-$(nproc)}
IFS=', ' read -ra MODES <<< "${MODES:-vanilla pb cgpb sbridge}"   # comma- or space-separated
IFS=', ' read -ra EXCL <<< "${EXCLUDE:-}"   # exclude whole modes ("sbridge") AND/OR
                                            # individual cells ("sbridge_cpd2"); comma/space-sep
is_excluded() {  # $1=mode $2=cpd ; true if "$mode" or "${mode}_cpd${cpd}" is listed
  [ "${#EXCL[@]}" -eq 0 ] && return 1
  local e
  for e in "${EXCL[@]}"; do
    if [ "$e" = "$1" ] || [ "$e" = "${1}_cpd${2}" ]; then return 0; fi
  done
  return 1
}
CPDS=(2 3 4 5 6 7 8)
OUT=${OUT:-$HOME/bagsize_sample_sweep}

go build -o bin/trace_sim_go ./cmd/trace_sim || exit 1
mkdir -p "$OUT"
SARG=""; [ "$SAMPLE" -gt 0 ] && SARG="--sample $SAMPLE --sample-seed $SAMPLE_SEED"
echo "corpus=$CORPUS sample=$SAMPLE seed=$SAMPLE_SEED maxcores=$MAXCORES modes=${MODES[*]} -> $OUT"

t0=$SECONDS
for mode in "${MODES[@]}"; do
  for cpd in "${CPDS[@]}"; do
    if is_excluded "$mode" "$cpd"; then echo "skip $mode cpd=$cpd (excluded)"; continue; fi
    ( ./bin/trace_sim_go --corpus "$CORPUS" --bagsize --mode "$mode" --checkpoint-distance "$cpd" --progress "$PROGRESS" $SARG \
        -o "$OUT/${mode}_cpd${cpd}.json" 2> "$OUT/${mode}_cpd${cpd}.err"
      echo "done $mode cpd=$cpd" ) &
    while [ "$(jobs -rp | wc -l)" -ge "$MAXCORES" ]; do wait -n; done
  done
done
wait
echo "sweep done in $((SECONDS-t0))s"

# ---- aggregate: corpus-level overhead per cell ----
python3 - "$OUT" <<'PY' | tee "$OUT/summary.txt"
import json, sys, os, glob
out = sys.argv[1]
def wavg(vals, wts):
    s = sum(wts)
    return (sum(v*w for v, w in zip(vals, wts)) / s) if s else 0.0
# Discover every cell PRESENT in the dir (not just this run's modes), so a
# summary after incremental/partial runs includes all modes that have output.
cells = []
for f in glob.glob(os.path.join(out, "*_cpd*.json")):
    mode, _, cpd = os.path.basename(f)[:-5].rpartition("_cpd")
    try:
        cells.append((mode, int(cpd), f))
    except ValueError:
        continue
cells.sort(key=lambda c: (c[0], c[1]))
print(f"{'mode':<9}{'cpd':<4}{'payload_B/span':>15}{'payload_B/ckpt':>16}{'baggage_B/call':>16}")
for mode, cpd, f in cells:
    try:
        d = json.load(open(f))
    except Exception:
        print(f"{mode:<9}{cpd:<4}{'FAIL':>15}"); continue
    ns, ncs, nbc = d["num_spans"], d["num_checkpoint_spans"], d["num_baggage_calls"]
    pps = wavg(d["amortized_by_total"], ns)        # payload bytes / span
    ppc = wavg(d["amortized_by_checkpoint"], ncs)  # payload bytes / checkpoint-span
    bpc = wavg(d["avg_baggage_call"], nbc)         # baggage bytes / call
    print(f"{mode:<9}{cpd:<4}{pps:>15.3f}{ppc:>16.3f}{bpc:>16.3f}")
PY
echo "summary -> $OUT/summary.txt"
