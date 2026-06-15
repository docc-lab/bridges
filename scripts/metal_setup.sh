#!/usr/bin/env bash
# Bootstrap + run the CGPRB sweep on a single big x86 box (e.g. AWS c6a.metal,
# Ubuntu 22.04, 192 vCPU). Copy/paste the sections you need — not meant to run
# end-to-end blindly. Assumes you're the `ubuntu` user with sudo, and the four
# data files have been rsync'd to /mydata/uber/ (see DATA section).
set -euo pipefail

REPO_URL="<your-repo-git-url>"   # <-- set this
REPO_DIR="$HOME/bridges"

############################################################
# 1. System build deps (g++ for the OR-Tools cgo shim; NOT clang)
############################################################
sudo apt-get update
sudo apt-get install -y build-essential g++ git wget curl ca-certificates pkg-config rsync

############################################################
# 2. Go (match the build box: 1.26.4)
############################################################
cd /tmp
wget -q https://go.dev/dl/go1.26.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.26.4.linux-amd64.tar.gz
grep -q '/usr/local/go/bin' ~/.bashrc || echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
export PATH=$PATH:/usr/local/go/bin
go version    # -> go1.26.4 linux/amd64

############################################################
# 3. OR-Tools prebuilt (x86_64 Ubuntu 22.04, v9.15.6755), under your home.
#    Confirm the exact asset name on github.com/google/or-tools/releases/tag/v9.15
############################################################
cd "$HOME"
wget -q https://github.com/google/or-tools/releases/download/v9.15/or-tools_amd64_ubuntu-22.04_cpp_v9.15.6755.tar.gz
tar xzf or-tools_amd64_ubuntu-22.04_cpp_v9.15.6755.tar.gz
ls "$HOME/or-tools_x86_64_Ubuntu-22.04_cpp_v9.15.6755/lib/libortools.so"   # sanity

############################################################
# 4. Clone repo, repoint the baked OR-Tools path to your home
############################################################
git clone "$REPO_URL" "$REPO_DIR"
cd "$REPO_DIR"
sed -i "s#/users/tomislav#$HOME#g" recon/cpsat_cgo.go recon/fullsat_cgo.go

############################################################
# 5. Build the cpsat binary — the key OR-Tools-link gate
############################################################
go build -tags cpsat -o bin/trace_recon_cgprb ./cmd/trace_recon && echo BUILD_OK
go build -o bin/archive_to_store ./cmd/archive_to_store

############################################################
# 6. DATA: the four files must already be at these paths (rsync'd from CloudLab)
#    /mydata/uber/corpus_full_unfiltered.arc       + corpus_full_unfiltered/meta.bin
#    /mydata/uber/corpus_day2_unfiltered.arc       + corpus_day2_unfiltered/meta.bin
#    (make /mydata yours first:  sudo chown -R "$USER":"$USER" /mydata )
############################################################

############################################################
# 7. Smoke test: build day-1 store once, run one capped cell (~30s -> a TOPO line)
############################################################
./bin/archive_to_store --archive /mydata/uber/corpus_full_unfiltered.arc \
  --meta /mydata/uber/corpus_full_unfiltered/meta.bin \
  --store /mydata/uber/day1.store --meta-out /mydata/uber/day1_meta --progress 100000

TRACE_RECON_TOPO=1 TRACE_RECON_CPSAT=1 ./bin/trace_recon_cgprb \
  --corpus /mydata/uber/day1_meta --trace-store /mydata/uber/day1.store \
  --mode cgprb --checkpoint-distance 6 --drop-rate 0.75 --tie-policy aware --per-trace-drop-seed \
  --trace-count 20000 --workers 32 -o /tmp/smoke.json 2>&1 | grep -E '^TOPO|error|panic'

############################################################
# 8. Full sweep (both days; 6 cells x 32 workers concurrent on 192 vCPU)
############################################################
bash scripts/run_cgprb_sweep_metal.sh           # or: ... day1
tail -f "$HOME/cgprb_sweep_metal/summary.log"
