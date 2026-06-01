"""Analyze whether trace size/structure or per-service activity correlates
with dangling-parent rate.

Reads:
  traces.csv    (one row per trace; produced by cmd/arrival_loss_extract)
  services.csv  (one row per service; same producer)

Reports three things:

  (1) Within-trace concurrency / structural complexity:
      For each of num_spans, max_concurrent_spans, max_depth, max_fanout,
      span_emission_rate (num_spans / trace_duration_s), compute
        - Spearman correlation with has_dangling (binary)
        - point-biserial correlation
      Plus a "P(has_dangling) per decile of load-proxy" plot.

  (2) Per-service activity:
      For each service, dangling_rate = total_dangling_spans / total_spans.
      Correlate dangling_rate vs total_spans (Spearman + Pearson) and
      vs total_traces. Scatter plot in log space.

  (3) Trace size summary: same plots as (1) but isolated for the "size"
      proxies (num_spans, max_depth, max_fanout) versus "load-instant"
      proxies (max_concurrent_spans, span_emission_rate).

Usage:

    python scripts/arrival_loss_analyze.py \\
        --traces-csv /mydata/uber/results_full/arrival_loss/traces.csv \\
        --services-csv /mydata/uber/results_full/arrival_loss/services.csv \\
        --out-dir /mydata/uber/results_full/arrival_loss/plots
"""

import argparse
import os

import numpy as np
import pandas as pd
from scipy import stats
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ---------- I/O ----------

def load_traces(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={
        "start_us": np.int64,
        "end_us": np.int64,
        "trace_duration_us": np.int64,
        "num_spans": np.int32,
        "num_dangling_spans": np.int32,
        "max_concurrent_spans": np.int32,
        "max_depth": np.int32,
        "max_fanout": np.int32,
    })
    df["has_dangling"] = (df["num_dangling_spans"] > 0).astype(np.int8)
    # Emission rate: spans per second over the trace's wall-clock duration.
    # When duration == 0 (single instant) we set NaN and drop from that
    # particular correlation later.
    dur_s = df["trace_duration_us"].astype(np.float64) / 1e6
    df["span_emission_rate"] = np.where(dur_s > 0, df["num_spans"] / dur_s, np.nan)
    return df


def load_services(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["dangling_rate"] = np.where(df["total_spans"] > 0,
                                   df["total_dangling_spans"] / df["total_spans"], 0.0)
    return df


# ---------- correlations ----------

def correlate(name: str, x: np.ndarray, y_binary: np.ndarray) -> dict:
    """Spearman + point-biserial between continuous x and binary y."""
    mask = np.isfinite(x)
    x = x[mask]
    y = y_binary[mask]
    if len(x) < 100 or y.std() == 0:
        return {"name": name, "n": len(x), "spearman": np.nan, "pb": np.nan,
                "p_spearman": np.nan, "p_pb": np.nan}
    sp = stats.spearmanr(x, y)
    pb = stats.pointbiserialr(y.astype(int), x)
    return {
        "name": name, "n": len(x),
        "spearman": sp.statistic, "p_spearman": sp.pvalue,
        "pb": pb.statistic, "p_pb": pb.pvalue,
    }


def decile_lift(x: np.ndarray, y_binary: np.ndarray, n_bins: int = 20):
    """Return (centers, p_dangling, n_per_bin) over quantile bins of x."""
    mask = np.isfinite(x)
    x = x[mask]
    y = y_binary[mask].astype(np.float64)
    if len(x) < n_bins * 5:
        return np.array([]), np.array([]), np.array([])
    qs = np.linspace(0, 1, n_bins + 1)
    edges = np.unique(np.quantile(x, qs))
    if len(edges) < 3:
        return np.array([]), np.array([]), np.array([])
    bins = np.clip(np.searchsorted(edges, x, side="right") - 1, 0, len(edges) - 2)
    centers, rates, ns = [], [], []
    for b in range(len(edges) - 1):
        m = bins == b
        if m.sum() < 10:
            continue
        centers.append(np.median(x[m]))
        rates.append(y[m].mean())
        ns.append(m.sum())
    return np.array(centers), np.array(rates), np.array(ns)


# ---------- plotting ----------

def plot_decile(name: str, x: np.ndarray, y: np.ndarray, out_path: str,
                xlog: bool = True) -> None:
    centers, rates, ns = decile_lift(x, y)
    if len(centers) == 0:
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(centers, rates, marker="o", color="C3", lw=2)
    if xlog:
        ax.set_xscale("log")
    overall = y.mean()
    ax.axhline(overall, color="gray", ls="--", alpha=0.6,
               label=f"overall mean = {overall:.3%}")
    ax.set_xlabel(f"{name} (binned median)")
    ax.set_ylabel("P(trace has ≥1 dangling-parent span)")
    ax.set_title(f"Dangling-parent probability vs {name}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_service_scatter(services: pd.DataFrame, out_path: str,
                         x_col: str, title: str) -> None:
    s = services[services["total_spans"] >= 100].copy()
    if len(s) < 10:
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(s[x_col], s["dangling_rate"], s=10, alpha=0.4,
               edgecolors="none")
    ax.set_xscale("log")
    ax.set_xlabel(f"{x_col} (per service, log scale)")
    ax.set_ylabel("dangling_rate = total_dangling_spans / total_spans")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


# ---------- pipelines ----------

def run_trace_analysis(traces: pd.DataFrame, out_dir: str) -> list:
    results = []
    y = traces["has_dangling"].to_numpy()
    print(f"\n=== (1) Per-trace structure / load proxies ===")
    print(f"  n_traces = {len(traces):,}")
    print(f"  P(has ≥1 dangling) = {y.mean():.4%} (global)")
    print()
    print(f"  {'proxy':<28} {'n':>10} {'spearman':>12} {'p':>10} "
          f"{'point-bis':>12} {'p':>10}")
    cols = [
        ("num_spans", True),
        ("max_concurrent_spans", True),
        ("max_depth", False),
        ("max_fanout", True),
        ("span_emission_rate", True),
        ("trace_duration_us", True),
    ]
    for col, xlog in cols:
        x = traces[col].to_numpy(dtype=np.float64)
        r = correlate(col, x, y)
        results.append(r)
        print(f"  {col:<28} {r['n']:>10,} "
              f"{r['spearman']:>+12.4f} {r['p_spearman']:>10.2e} "
              f"{r['pb']:>+12.4f} {r['p_pb']:>10.2e}")
        plot_decile(col, x, y,
                    os.path.join(out_dir, f"decile_{col}.png"),
                    xlog=xlog)
    return results


def run_service_analysis(services: pd.DataFrame, out_dir: str) -> list:
    print(f"\n=== (2) Per-service activity ===")
    print(f"  n_services = {len(services):,}")
    # Drop tiny services from correlation (noise floor).
    s = services[services["total_spans"] >= 100].copy()
    print(f"  n_services with >=100 spans = {len(s):,}")
    overall = (s["total_dangling_spans"].sum() / max(s["total_spans"].sum(), 1))
    print(f"  Pooled dangling rate over these services = {overall:.4%}")
    out = []
    for x_col in ["total_spans", "total_traces"]:
        x = s[x_col].to_numpy(dtype=np.float64)
        y = s["dangling_rate"].to_numpy(dtype=np.float64)
        if len(x) >= 10 and y.std() > 0:
            sp = stats.spearmanr(x, y)
            pe = stats.pearsonr(np.log10(x), y)  # log-x for skewed counts
            print(f"  dangling_rate vs {x_col}: "
                  f"Spearman r = {sp.statistic:+.4f} (p={sp.pvalue:.2e}); "
                  f"Pearson(log {x_col}) r = {pe.statistic:+.4f} (p={pe.pvalue:.2e})")
            out.append({"x_col": x_col, "spearman": sp.statistic,
                        "p_spearman": sp.pvalue, "pearson_log": pe.statistic,
                        "p_pearson_log": pe.pvalue})
        plot_service_scatter(services,
                             os.path.join(out_dir, f"services_{x_col}.png"),
                             x_col,
                             f"Per-service dangling rate vs {x_col}")
    # Top-30 worst-offender services (by dangling_rate among >=100 spans)
    top = s.sort_values("dangling_rate", ascending=False).head(30)
    print(f"\n  Top-30 services by dangling_rate (≥100 spans):")
    print(f"    {'service':<25} {'spans':>10} {'dangling':>10} {'rate':>8}")
    for _, row in top.iterrows():
        print(f"    {row['service_name']:<25} {int(row['total_spans']):>10,} "
              f"{int(row['total_dangling_spans']):>10,} "
              f"{row['dangling_rate']:>7.2%}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--traces-csv", required=True)
    ap.add_argument("--services-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    traces = load_traces(args.traces_csv)
    services = load_services(args.services_csv)

    trace_results = run_trace_analysis(traces, args.out_dir)
    svc_results = run_service_analysis(services, args.out_dir)

    # Persist a compact summary.
    with open(os.path.join(args.out_dir, "correlations.txt"), "w") as fh:
        fh.write("# trace-level correlations (proxy vs has_dangling)\n")
        fh.write("proxy\tn\tspearman\tp_spearman\tpoint_biserial\tp_pb\n")
        for r in trace_results:
            fh.write(f"{r['name']}\t{r['n']}\t{r['spearman']:.4f}\t"
                     f"{r['p_spearman']:.3e}\t{r['pb']:.4f}\t{r['p_pb']:.3e}\n")
        fh.write("\n# service-level correlations (proxy vs dangling_rate)\n")
        fh.write("proxy\tspearman\tp\tpearson_log\tp\n")
        for r in svc_results:
            fh.write(f"{r['x_col']}\t{r['spearman']:.4f}\t{r['p_spearman']:.3e}\t"
                     f"{r['pearson_log']:.4f}\t{r['p_pearson_log']:.3e}\n")
    print(f"\nWrote plots and correlations.txt to {args.out_dir}")


if __name__ == "__main__":
    main()
