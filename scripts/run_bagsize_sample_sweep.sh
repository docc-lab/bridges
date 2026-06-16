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
CPDS=(2 3 4 5 6 7 8)
OUT=${OUT:-$HOME/bagsize_sample_sweep}

go build -o bin/trace_sim_go ./cmd/trace_sim || exit 1
mkdir -p "$OUT"
SARG=""; [ "$SAMPLE" -gt 0 ] && SARG="--sample $SAMPLE --sample-seed $SAMPLE_SEED"
echo "corpus=$CORPUS sample=$SAMPLE seed=$SAMPLE_SEED maxcores=$MAXCORES modes=${MODES[*]} -> $OUT"

t0=$SECONDS
for mode in "${MODES[@]}"; do
  for cpd in "${CPDS[@]}"; do
    ( ./bin/trace_sim_go --corpus "$CORPUS" --bagsize --mode "$mode" --checkpoint-distance "$cpd" --progress "$PROGRESS" $SARG \
        -o "$OUT/${mode}_cpd${cpd}.json" 2> "$OUT/${mode}_cpd${cpd}.err"
      echo "done $mode cpd=$cpd" ) &
    while [ "$(jobs -rp | wc -l)" -ge "$MAXCORES" ]; do wait -n; done
  done
done
wait
echo "sweep done in $((SECONDS-t0))s"

# ---- aggregate: corpus-level overhead per cell ----
python3 - "$OUT" "${MODES[*]}" "${CPDS[*]}" <<'PY' | tee "$OUT/summary.txt"
import json, sys, os
out, modes, cpds = sys.argv[1], sys.argv[2].split(), sys.argv[3].split()
def wavg(vals, wts):
    s = sum(wts)
    return (sum(v*w for v, w in zip(vals, wts)) / s) if s else 0.0
print(f"{'mode':<9}{'cpd':<4}{'payload_B/span':>15}{'payload_B/ckpt':>16}{'baggage_B/call':>16}")
for mode in modes:
    for cpd in cpds:
        f = os.path.join(out, f"{mode}_cpd{cpd}.json")
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
