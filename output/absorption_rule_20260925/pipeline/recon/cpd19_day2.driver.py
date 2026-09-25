#!/usr/bin/env python3
"""CPD 1:9 / depth_cubic reconstruction sweep driver.

Invariants (the whole point of this script):
  * exactly one trace computation per PHYSICAL core, for that job's entire life
  * SMT siblings of every busy core stay idle -- we only ever schedule on the
    low half of each (package, core) pair and never touch the high sibling
  * a core is returned to the pool only after its job has fully exited
"""
import argparse, json, os, subprocess, sys, time
from pathlib import Path

ROOT = Path("/home/ubuntu/sweep_day2_final")
BIN  = ROOT / "bin" / "trace_recon"
SHARDROOT = Path("/dev/shm/day2_shards")

MODES = ["pb0", "cgp0", "sb3"]
ARMS  = ["forward", "reverse"]

def topology():
    """Map logical cpu -> (package, core). Return the one-cpu-per-physical set."""
    pairs = {}
    for c in range(os.cpu_count()):
        base = Path(f"/sys/devices/system/cpu/cpu{c}/topology")
        try:
            pkg = (base / "physical_package_id").read_text().strip()
            core = (base / "core_id").read_text().strip()
        except OSError:
            continue
        pairs.setdefault((pkg, core), []).append(c)
    primary = {min(v): k for k, v in pairs.items()}      # lowest cpu of each pair
    siblings = {min(v): [x for x in v if x != min(v)] for v in pairs.values()}
    return primary, siblings

def build_jobs(shards):
    jobs = []
    for shard in shards:
        for mode in MODES:
            for arm in ARMS:
                jobs.append({"mode": mode, "arm": arm, "shard": shard,
                             "key": f"{mode}_{arm}_{shard}"})
    return jobs

def command(job, cpu):
    shard = SHARDROOT / job["shard"]
    key = job["key"]
    res = ROOT / "resources" / f"{key}.resources.json"
    cmd = [
        "taskset", "-c", str(cpu),
        "/usr/bin/time",
        "-f", '{"wall_seconds":%e,"user_seconds":%U,"system_seconds":%S,'
              '"max_rss_kib":%M,"exit_status":%x}',
        "-o", str(res),
        str(BIN),
        "--corpus", str(shard / "corpus"),
        "--trace-store", str(shard / "trace.store"),
        "--serial-traces", "--workers", "1",
        "--mode", job["mode"],
        "--checkpoint-range", "1:9", "--checkpoint-seed", "42",
        "--prefix-len", "8", "--bloom-fp", "0.0001", "--fp-bits", "64",
        "--prime-m=false", "--prime-m-bytecap=false",
        "--drop-rates", "0.05,0.25,0.5,0.75,0.95,1",
        "--per-trace-drop-seed", "--seed", "42",
        "--progress", "2000",
        "--timing", str(ROOT / "recon" / f"{key}_{{dc}}.csv"),
        "-o", str(ROOT / "recon" / f"{key}.json"),
    ]
    if job["arm"] == "reverse":
        cmd += ["--reverse-policy", "depth_cubic", "--leaf-reject", "1",
                "--reverse-seed", "42"]
    return cmd

def check_affinity(pid, cpu):
    """Assert every thread of pid is pinned to exactly {cpu}."""
    bad = []
    try:
        for t in Path(f"/proc/{pid}/task").iterdir():
            txt = (t / "status").read_text()
            for line in txt.splitlines():
                if line.startswith("Cpus_allowed_list:"):
                    got = line.split(":", 1)[1].strip()
                    if got != str(cpu):
                        bad.append({"tid": t.name, "expected": str(cpu), "got": got})
    except (OSError, FileNotFoundError):
        pass
    return bad

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--concurrency", type=int, required=True)
    ap.add_argument("--shards", default="all")
    ap.add_argument("--modes", default="")
    ap.add_argument("--arms", default="")
    ap.add_argument("--reserve", type=int, default=1,
                    help="how many low physical cores to keep free for the controller")
    args = ap.parse_args()

    primary, siblings = topology()
    pool_all = sorted(primary)[args.reserve:]
    pool = pool_all[: args.concurrency]
    if len(pool) < args.concurrency:
        sys.exit(f"only {len(pool)} physical cores available, asked {args.concurrency}")
    # hard assert: distinct (package, core), and no sibling of a pool cpu is in the pool
    seen = {}
    for c in pool:
        pc = primary[c]
        if pc in seen:
            sys.exit(f"ABORT: cpu{c} shares (package,core)={pc} with cpu{seen[pc]}")
        seen[pc] = c
    sib_set = {s for c in pool for s in siblings[c]}
    if sib_set & set(pool):
        sys.exit("ABORT: a pool cpu is the SMT sibling of another pool cpu")

    shards = sorted(p.name for p in SHARDROOT.iterdir() if p.is_dir())
    if args.shards != "all":
        want = set(args.shards.split(","))
        shards = [s for s in shards if s in want]
    global MODES, ARMS
    if args.modes: MODES = args.modes.split(",")
    if args.arms:  ARMS = args.arms.split(",")

    jobs = build_jobs(shards)
    print(f"[driver] {len(jobs)} jobs, concurrency {len(pool)}", flush=True)
    print(f"[driver] cpus {pool[0]}..{pool[-1]}; siblings left idle: "
          f"{min(sib_set)}..{max(sib_set)}", flush=True)

    free = list(pool)
    running = {}      # pid -> (job, cpu, proc, logf, start)
    results = []
    affinity_errors = []
    queue = list(jobs)
    started = time.time()

    while queue or running:
        while queue and free:
            cpu = free.pop(0)
            job = queue.pop(0)
            logp = ROOT / "logs" / f"{job['key']}.log"
            logf = open(logp, "wb")
            proc = subprocess.Popen(command(job, cpu), stdout=subprocess.DEVNULL,
                                    stderr=logf, env={**os.environ, "GOMAXPROCS": "1"})
            running[proc.pid] = (job, cpu, proc, logf, time.time())
            time.sleep(0.05)
            bad = check_affinity(proc.pid, cpu)
            if bad:
                affinity_errors.append({"key": job["key"], "cpu": cpu, "threads": bad})
                print(f"[AFFINITY] {job['key']} cpu{cpu}: {bad}", flush=True)

        time.sleep(2)
        for pid in list(running):
            job, cpu, proc, logf, t0 = running[pid]
            if proc.poll() is None:
                continue
            logf.close()
            rc = proc.returncode
            results.append({"key": job["key"], "mode": job["mode"], "arm": job["arm"],
                            "shard": job["shard"], "cpu": cpu,
                            "package_core": primary[cpu],
                            "exit": rc, "seconds": round(time.time() - t0, 1)})
            done, total = len(results), len(jobs)
            print(f"[{done}/{total}] {job['key']} cpu{cpu} exit={rc} "
                  f"{round(time.time()-t0,1)}s", flush=True)
            del running[pid]
            free.append(cpu)          # core released only now, after full exit

        # periodic affinity re-verification of everything still running
        for pid, (job, cpu, proc, _, _) in list(running.items()):
            if proc.poll() is None:
                bad = check_affinity(pid, cpu)
                if bad:
                    affinity_errors.append({"key": job["key"], "cpu": cpu, "threads": bad})

    manifest = {
        "config": {
            "checkpoint_range": "1:9", "checkpoint_seed": 42, "policy": "depth_cubic",
            "prefix_len": 8, "bloom_fp": 0.0001, "fp_bits": 64, "prime": False,
            "drop_rates": [0.05, 0.25, 0.5, 0.75, 0.95, 1.0], "seed": 42,
            "per_trace_drop_seed": True, "modes": MODES, "arms": ARMS,
        },
        "binary_sha256": subprocess.run(["sha256sum", str(BIN)],
                                        capture_output=True, text=True).stdout.split()[0],
        "concurrency": len(pool), "cpus": pool,
        "siblings_left_idle": sorted(sib_set),
        "jobs": len(jobs), "shards": shards,
        "wall_seconds": round(time.time() - started, 1),
        "affinity_errors": affinity_errors,
        "results": results,
        "status": "complete" if all(r["exit"] == 0 for r in results) and not affinity_errors
                  else "PROBLEMS",
    }
    (ROOT / "provenance" / "run_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"[driver] status={manifest['status']} wall={manifest['wall_seconds']}s "
          f"affinity_errors={len(affinity_errors)}", flush=True)

if __name__ == "__main__":
    main()
