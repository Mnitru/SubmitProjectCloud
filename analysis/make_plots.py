#!/usr/bin/env python3
"""Build every figure and summary table from the raw results.

    python analysis/make_plots.py [results_dir]

Inputs : results/runs.jsonl, results/codec_bench.csv
Outputs: results/figures/*.png and results/tables/*.csv
Error bars = standard deviation over the repetitions (n shown in the tables).
"""
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

RES = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")
FIG = os.path.join(RES, "figures")
TAB = os.path.join(RES, "tables")
os.makedirs(FIG, exist_ok=True)
os.makedirs(TAB, exist_ok=True)

# ---- palette: reference categorical slots in fixed order; baseline = neutral ink
INK, INK2, MUTED, GRID, BASE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
S1, S2, S3, S4 = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
CODEC_COLOR = {"none": INK2, "off": INK2, "lz4": S1, "zstd": S4}
CODEC_MARK = {"none": "o", "off": "o", "lz4": "s", "zstd": "v"}
APP_COLOR = {"sort": S1, "cpu": S2, "mixed": S3}
APP_LABEL = {"sort": "Sort (I/O-bound)", "cpu": "Hash (CPU-bound)", "mixed": "Join (balanced)"}
CFG_COLOR = {"C0": INK2, "C1": S1, "C2": S2, "C3": S3}
STORAGE_ORDER = ["none", "lz4", "zstd"]
SHUFFLE_ORDER = ["off", "lz4", "zstd"]
APPS = ["sort", "cpu", "mixed"]
C_FAMILY = "zstd"   # C1 = zstd/off, C2 = none/zstd, C3 = zstd/zstd

plt.rcParams.update({
    "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "savefig.facecolor": "#fcfcfb",
    "font.family": "DejaVu Sans", "font.size": 9, "axes.titlesize": 10, "axes.titleweight": "bold",
    "axes.edgecolor": BASE, "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False,
    "lines.linewidth": 2, "lines.markersize": 6, "errorbar.capsize": 3,
})
ERR = {"ecolor": INK2, "elinewidth": 1, "capsize": 3}


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, name + ".png"), dpi=160)
    plt.close(fig)
    print("  figure", name)


# ------------------------------------------------------------------ load
def load_runs():
    path = os.path.join(RES, "runs.jsonl")
    rows = []
    with open(path) as f:
        for line in f:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("status") != "ok":
                continue
            sp = r.get("spark") or {}
            wk = (r.get("system") or {}).get("workers") or {}
            ar = r.get("app_result") or {}
            rows.append({
                "run_id": r["run_id"], "suite": r.get("suite"), "app": r.get("app"),
                "size_gb": r.get("size_gb"), "storage": r.get("storage"), "shuffle": r.get("shuffle"),
                "rep": r.get("rep"), "wall_s": r.get("wall_s"),
                "time_s": sp.get("jobs_s"),
                "exec_cpu_s": sp.get("executor_cpu_s"), "exec_run_s": sp.get("executor_run_s"),
                "cpu_fraction": sp.get("cpu_fraction"),
                "gc_s": sp.get("gc_s"),
                "input_bytes": sp.get("input_bytes"), "output_bytes": sp.get("output_bytes"),
                "shuffle_write_bytes": sp.get("shuffle_write_bytes"),
                "shuffle_remote_bytes": sp.get("shuffle_remote_bytes"),
                "shuffle_write_time_s": sp.get("shuffle_write_time_s"),
                "fetch_wait_s": sp.get("shuffle_fetch_wait_s"),
                "shuffle_read_stages_s": sp.get("shuffle_read_stages_s"),
                "cpu_s": wk.get("cpu_busy_s"), "iowait_s": wk.get("cpu_iowait_s"),
                "cpu_util": wk.get("cpu_util_pct_avg"), "iowait_pct": wk.get("iowait_pct_avg"),
                "disk_read": wk.get("disk_read_bytes"), "disk_write": wk.get("disk_write_bytes"),
                "net_tx": wk.get("net_tx_bytes"), "net_rx": wk.get("net_rx_bytes"),
                "disk_MBps": (wk.get("disk_read_MBps_avg") or 0) + (wk.get("disk_write_MBps_avg") or 0),
                "net_MBps": wk.get("net_tx_MBps_avg"),
                "events_bytes": ar.get("events_bytes"), "users_bytes": ar.get("users_bytes"),
                "write_s": (ar.get("events_write_s") or 0) + (ar.get("users_write_s") or 0) if "events_write_s" in ar else None,
            })
    df = pd.DataFrame(rows)
    for c in ("disk_read", "disk_write"):
        df[c] = df[c].fillna(0)
    df["disk_bytes"] = df["disk_read"] + df["disk_write"]
    return df


def agg(df, keys, col):
    g = df.groupby(keys)[col].agg(["mean", "std", "count"]).reset_index()
    g["std"] = g["std"].fillna(0)
    return g


def config_label(storage, shuffle):
    if storage == "none" and shuffle == "off":
        return "C0"
    if storage == C_FAMILY and shuffle == "off":
        return "C1"
    if storage == "none" and shuffle == C_FAMILY:
        return "C2"
    if storage == C_FAMILY and shuffle == C_FAMILY:
        return "C3"
    return None


def line_panel(ax, sub, series_col, order, ycol, scale=1.0):
    """Lines vs dataset size, one per codec, mean +/- sd, label at line end."""
    g = agg(sub, ["size_gb", series_col], ycol)
    for s in order:
        d = g[g[series_col] == s].sort_values("size_gb")
        if d.empty:
            continue
        ax.errorbar(d["size_gb"], d["mean"] * scale, yerr=d["std"] * scale, color=CODEC_COLOR[s],
                    marker=CODEC_MARK[s], label=s, elinewidth=1)
    ax.set_xlabel("Dataset size (GB)")
    sizes = sorted(sub["size_gb"].dropna().unique())
    ax.set_xticks(sizes)
    ax.set_xlim(0, max(sizes) * 1.08 if sizes else 1)
    ax.set_ylim(bottom=0)


def sweep_figure(main, ycol, ylabel, name, scale=1.0, which=("storage", "shuffle")):
    """For each app: storage-codec sweep (shuffle off) and shuffle-codec sweep (storage none)."""
    rows = len(which)
    fig, axes = plt.subplots(rows, 3, figsize=(12, 3.3 * rows), squeeze=False)
    for r, kind in enumerate(which):
        for c, app in enumerate(APPS):
            ax = axes[r][c]
            a = main[main["app"] == app]
            if kind == "storage":
                sub, col, order = a[a["shuffle"] == "off"], "storage", STORAGE_ORDER
                ttl = "storage codec, shuffle uncompressed"
            else:
                sub, col, order = a[a["storage"] == "none"], "shuffle", SHUFFLE_ORDER
                ttl = "shuffle codec, storage uncompressed"
            line_panel(ax, sub, col, order, ycol, scale)
            ax.set_title(f"{APP_LABEL[app]}\n{ttl}", fontsize=9)
            ax.set_ylabel(ylabel)
            if c == 2:
                ax.legend(title=col, fontsize=8, title_fontsize=8, loc="upper left", bbox_to_anchor=(1.02, 1))
    save(fig, name)


# ------------------------------------------------------------------ figures
def fig_characterization(df):
    char = df[(df["suite"].isin(["char", "main"])) & (df["storage"] == "none") & (df["shuffle"] == "off")]
    if char.empty:
        return
    size = char["size_gb"].max()
    char = char[char["size_gb"] == size]
    metrics = [("cpu_util", "Worker CPU utilisation (%)"), ("iowait_pct", "CPU iowait (%)"),
               ("cpu_fraction", "Executor CPU time / run time"),
               ("disk_MBps", "Disk I/O per worker (MB/s)"), ("net_MBps", "Network out per worker (MB/s)")]
    fig, axes = plt.subplots(1, len(metrics), figsize=(14, 3))
    for ax, (col, lab) in zip(axes, metrics):
        g = agg(char, ["app"], col).set_index("app").reindex(APPS)
        ax.bar(range(len(APPS)), g["mean"], yerr=g["std"], color=[APP_COLOR[a] for a in APPS], width=0.6,
               edgecolor="#fcfcfb", linewidth=2, error_kw=ERR)
        ax.set_xticks(range(len(APPS)))
        ax.set_xticklabels(["Sort\nI/O", "Hash\nCPU", "Join\nmixed"])
        ax.set_title(lab, fontsize=9)
        for i, (v, s) in enumerate(zip(g["mean"], g["std"])):
            if pd.notna(v):
                txt = f"{v:.2f}" if col == "cpu_fraction" else (f"{v:.1f}" if v < 10 else f"{v:.0f}")
                ax.annotate(txt, (i, v + (s if pd.notna(s) else 0)), xytext=(0, 3),
                            textcoords="offset points", ha="center", fontsize=8, color=INK2)
    fig.suptitle(f"Workload characterisation - baseline C0 (no compression), {size:g} GB, mean ± sd",
                 fontsize=10, fontweight="bold")
    save(fig, "01_workload_characterisation")
    tab = char.groupby("app")[["time_s", "cpu_util", "iowait_pct", "cpu_fraction", "disk_MBps", "net_MBps",
                               "cpu_s", "disk_bytes", "net_tx", "shuffle_write_bytes"]].mean()
    tab.to_csv(os.path.join(TAB, "workload_characterisation.csv"))


def fig_timeline(df):
    """CPU / disk / network over time for one baseline run of each app."""
    base = df[(df["storage"] == "none") & (df["shuffle"] == "off") & df["suite"].isin(["char", "main"])]
    if base.empty:
        return
    size = base["size_gb"].max()
    fig, axes = plt.subplots(3, 3, figsize=(13, 7), sharex="col")
    for c, app in enumerate(APPS):
        runs = base[(base["app"] == app) & (base["size_gb"] == size)]
        if runs.empty:
            continue
        ts_path = os.path.join(RES, "raw", runs.iloc[0]["run_id"], "timeseries.csv")
        if not os.path.exists(ts_path):
            continue
        ts = pd.read_csv(ts_path)
        ts = ts[ts["node"].str.startswith("worker")]
        for r, (metrics, lab) in enumerate([(["cpu_pct"], "CPU busy (%)"),
                                            (["disk_read_MBps", "disk_write_MBps"], "Disk (MB/s)"),
                                            (["net_tx_MBps"], "Network out (MB/s)")]):
            ax = axes[r][c]
            for m in metrics:
                s = ts[ts["metric"] == m].groupby("t_rel_s")["value"].mean()
                ax.plot(s.index, s.values, lw=1.5, color=S1 if m != "disk_write_MBps" else S2,
                        label=m.replace("_MBps", "").replace("_", " "))
            ax.set_ylabel(lab)
            ax.set_ylim(bottom=0)
            if r == 0:
                ax.set_title(APP_LABEL[app])
                ax.set_ylim(0, 100)
            if len(metrics) > 1 and c == 2:
                ax.legend(fontsize=8)
        axes[2][c].set_xlabel("Time since start (s)")
    fig.suptitle(f"Resource usage over time, average of the 3 workers - baseline, {size:g} GB",
                 fontsize=10, fontweight="bold")
    save(fig, "02_resource_timelines")


def fig_codec_bench():
    path = os.path.join(RES, "codec_bench.csv")
    if not os.path.exists(path):
        return
    cb = pd.read_csv(path)
    g = cb.groupby(["input", "block_kb", "codec"]).agg(
        ratio=("ratio", "mean"), comp=("compress_MBps", "mean"), comp_sd=("compress_MBps", "std"),
        dec=("decompress_MBps", "mean"), dec_sd=("decompress_MBps", "std"),
        ccpu=("compress_cpu_s", "mean"), dcpu=("decompress_cpu_s", "mean"),
        orig=("original_bytes", "first")).reset_index()
    g["cpu_ns_per_byte_c"] = g["ccpu"] / g["orig"] * 1e9
    g["cpu_ns_per_byte_d"] = g["dcpu"] / g["orig"] * 1e9
    g.to_csv(os.path.join(TAB, "codec_intrinsic.csv"), index=False)
    order = ["lz4", "zstd-1", "zstd-3", "zstd-9"]
    fam = lambda c: c.split("-")[0]  # noqa: E731

    fig, axes = plt.subplots(1, 3, figsize=(13, 3.6))
    sub = g[g["block_kb"] == 1024]
    inputs = list(sub["input"].unique())
    width = 0.8 / max(1, len(inputs))
    for k, inp in enumerate(inputs):
        d = sub[sub["input"] == inp].set_index("codec").reindex(order)
        x = np.arange(len(order)) + (k - (len(inputs) - 1) / 2) * width
        axes[0].bar(x, d["ratio"], width=width, color=[S1, S2][k % 2], edgecolor="#fcfcfb", linewidth=1.5,
                    label=inp)
        axes[1].bar(x, d["comp"], width=width, yerr=d["comp_sd"], color=[S1, S2][k % 2],
                    edgecolor="#fcfcfb", linewidth=1.5, label=inp)
        axes[2].bar(x, d["dec"], width=width, yerr=d["dec_sd"], color=[S1, S2][k % 2],
                    edgecolor="#fcfcfb", linewidth=1.5, label=inp)
    for ax, t, yl in zip(axes, ["Compression ratio", "Compression speed", "Decompression speed"],
                         ["original / compressed", "MB/s (1 thread)", "MB/s (1 thread)"]):
        ax.set_xticks(range(len(order)))
        ax.set_xticklabels(order, rotation=30)
        ax.set_title(t)
        ax.set_ylabel(yl)
    axes[0].legend(fontsize=8)
    fig.suptitle("Intrinsic codec characteristics (1 MB blocks, mean ± sd of 3 runs)", fontsize=10, fontweight="bold")
    save(fig, "03_codec_intrinsic")

    fig, ax = plt.subplots(figsize=(6.5, 4.2))
    d = g[(g["block_kb"] == 1024) & (g["input"] == "sample.csv")]
    for _, r in d.iterrows():
        ax.scatter(r["comp"], r["ratio"], s=110, color=CODEC_COLOR.get(fam(r["codec"]), INK2),
                   marker=CODEC_MARK.get(fam(r["codec"]), "o"), edgecolor="#fcfcfb", linewidth=1.5, zorder=3)
        left = False
        ax.annotate(r["codec"], (r["comp"], r["ratio"]), xytext=(-8 if left else 8, 6 if left else -10),
                    textcoords="offset points", fontsize=9, color=INK2, ha="right" if left else "left")
    ax.set_xscale("log")
    ticks = [20, 50, 100, 200, 500]
    ax.set_xticks(ticks)
    ax.set_xticklabels([str(t) for t in ticks])
    ax.minorticks_off()
    ax.set_xlim(12, 900)
    ax.set_ylim(1.45, 3.15)
    ax.set_xlabel("Compression speed (MB/s, log scale) - faster is cheaper in CPU")
    ax.set_ylabel("Compression ratio")
    ax.set_title("Trade-off: ratio vs CPU cost (CSV sample, 1 MB blocks)")
    save(fig, "04_codec_tradeoff")


def fig_storage_overheads(df):
    # main cluster only: the additional 2-worker experiments (fastnet-*, random-*) write their own copies
    main_cluster = ~df["run_id"].str.startswith(("fastnet", "random"))
    w = df[(df["suite"] == "write") & main_cluster]
    s = df[(df["suite"] == "scan") & main_cluster]
    if w.empty and s.empty:
        return
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.6))
    if not w.empty:
        w = w.copy()
        base = w[w["storage"] == "none"].groupby("size_gb")["events_bytes"].mean()
        stored = w[w["storage"] != "none"].groupby(["size_gb", "storage"])[["events_bytes", "users_bytes"]].mean()
        rat = []
        for (size, codec), r in stored.iterrows():
            if size in base.index and r["events_bytes"]:
                rat.append({"size_gb": size, "storage": codec, "ratio": base[size] / r["events_bytes"],
                            "stored_GB": (r["events_bytes"] + r["users_bytes"]) / 1e9})
        rat = pd.DataFrame(rat)
        if not rat.empty:
            rat.to_csv(os.path.join(TAB, "hdfs_dataset_sizes.csv"), index=False)
            biggest = rat["size_gb"].max()
            d = rat[rat["size_gb"] == biggest].set_index("storage").reindex(STORAGE_ORDER[1:])
            axes[0].bar(range(len(d)), d["ratio"], color=[CODEC_COLOR[c] for c in d.index], width=0.6,
                        edgecolor="#fcfcfb", linewidth=2, error_kw=ERR)
            axes[0].set_xticks(range(len(d)))
            axes[0].set_xticklabels(d.index)
            for i, v in enumerate(d["ratio"]):
                axes[0].annotate(f"{v:.2f}x", (i, v), xytext=(0, 3), textcoords="offset points", ha="center",
                                 fontsize=8, color=INK2)
            axes[0].set_title(f"Parquet size reduction on HDFS ({biggest:g} GB)")
            axes[0].set_ylabel("uncompressed / compressed")
        line_panel(axes[1], w, "storage", STORAGE_ORDER, "time_s")
        axes[1].set_title("Write time (compression cost)")
        axes[1].set_ylabel("Execution time (s)")
    if not s.empty:
        line_panel(axes[2], s, "storage", STORAGE_ORDER, "time_s")
        axes[2].set_title("Full scan time (decompression cost)")
        axes[2].set_ylabel("Execution time (s)")
        axes[2].legend(fontsize=8, title="codec", title_fontsize=8)
    save(fig, "05_storage_write_read_overhead")


def fig_configs(main):
    m = main.copy()
    m["cfg"] = [config_label(s, c) for s, c in zip(m["storage"], m["shuffle"])]
    m = m[m["cfg"].notna()]
    if m.empty:
        return
    sizes = sorted(m["size_gb"].unique())
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.6))
    cfgs = ["C0", "C1", "C2", "C3"]
    width = 0.2
    for ax, app in zip(axes, APPS):
        g = agg(m[m["app"] == app], ["size_gb", "cfg"], "time_s")
        for k, cfg in enumerate(cfgs):
            d = g[g["cfg"] == cfg].set_index("size_gb").reindex(sizes)
            x = np.arange(len(sizes)) + (k - 1.5) * width
            ax.bar(x, d["mean"], width=width, yerr=d["std"], color=CFG_COLOR[cfg], label=cfg,
                   edgecolor="#fcfcfb", linewidth=1.5, error_kw=ERR)
        ax.set_xticks(range(len(sizes)))
        ax.set_xticklabels([f"{s:g} GB" for s in sizes])
        ax.set_title(APP_LABEL[app])
        ax.set_ylabel("Execution time (s)")
    axes[2].legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.02, 1))
    fig.suptitle(f"Combined compression: C0 none | C1 storage {C_FAMILY} | C2 shuffle {C_FAMILY} | C3 both",
                 fontsize=10, fontweight="bold")
    save(fig, "10_time_vs_config_C0_C3")


def fig_workload(main):
    """Execution time of every config normalised by C0, per workload type (largest size)."""
    size = main["size_gb"].max()
    m = main[main["size_gb"] == size]
    g = agg(m, ["app", "storage", "shuffle"], "time_s")
    rows = []
    for app in APPS:
        b = g[(g["app"] == app) & (g["storage"] == "none") & (g["shuffle"] == "off")]
        if b.empty:
            continue
        base = b["mean"].iloc[0]
        for _, r in g[g["app"] == app].iterrows():
            rows.append({"app": app, "config": f"{r['storage']}/{r['shuffle']}", "rel": r["mean"] / base,
                         "rel_sd": r["std"] / base})
    rel = pd.DataFrame(rows)
    if rel.empty:
        return
    rel.to_csv(os.path.join(TAB, f"relative_time_{size:g}GB.csv"), index=False)
    configs = [f"{s}/{c}" for s, c in [("lz4", "off"), ("zstd", "off"), ("none", "lz4"), ("none", "zstd"), ("zstd", "zstd")]]
    fig, ax = plt.subplots(figsize=(9, 3.6))
    width = 0.26
    for k, app in enumerate(APPS):
        d = rel[rel["app"] == app].set_index("config").reindex(configs)
        x = np.arange(len(configs)) + (k - 1) * width
        ax.bar(x, d["rel"], width=width, yerr=d["rel_sd"], color=APP_COLOR[app], label=APP_LABEL[app],
               edgecolor="#fcfcfb", linewidth=1.5, error_kw=ERR)
    ax.axhline(1.0, color=INK2, lw=1, ls="--")
    ax.set_xticks(range(len(configs)))
    ax.set_xticklabels([c.replace("/", "\n") for c in configs])
    ax.set_xlabel("storage codec / shuffle codec")
    ax.set_ylabel("Time / time without compression")
    ax.set_title(f"Each compression setting by workload ({size:g} GB): dashed line = no compression, "
                 f"below = faster", pad=24)
    ax.legend(fontsize=8, ncol=3, loc="lower left", bbox_to_anchor=(0, 1.0))
    save(fig, "11_time_vs_workload_type")


def fig_shuffle_detail(main):
    sub = main[main["storage"] == "none"]
    if sub.empty:
        return
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.6))
    size = sub["size_gb"].max()
    s = sub[sub["size_gb"] == size]
    for ax, (col, lab, sc) in zip(axes, [("shuffle_write_bytes", "Shuffle bytes written (MB)", 1e-6),
                                         ("shuffle_read_stages_s", "Duration of shuffle-read stages (s)", 1),
                                         ("fetch_wait_s", "Fetch wait time, all tasks (s)", 1)]):
        g = agg(s, ["app", "shuffle"], col)
        width = 0.26
        for k, codec in enumerate(SHUFFLE_ORDER):
            d = g[g["shuffle"] == codec].set_index("app").reindex(APPS)
            x = np.arange(len(APPS)) + (k - (len(SHUFFLE_ORDER) - 1) / 2) * width
            ax.bar(x, d["mean"] * sc, width=width, yerr=d["std"] * sc, color=CODEC_COLOR[codec], label=codec,
                   edgecolor="#fcfcfb", linewidth=1.5, error_kw=ERR)
        ax.set_xticks(range(len(APPS)))
        ax.set_xticklabels(["Sort", "Hash", "Join"])
        ax.set_title(lab, fontsize=9)
    axes[2].legend(fontsize=8, title="shuffle codec", title_fontsize=8)
    fig.suptitle(f"Shuffle detail at {size:g} GB (storage uncompressed)", fontsize=10, fontweight="bold")
    save(fig, "12_shuffle_detail")


def run_percentiles(run_id, q=0.95):
    """95th percentile of the 1 s samples per worker (mean over the workers), from the run's time series."""
    path = os.path.join(RES, "raw", run_id, "timeseries.csv")
    if not os.path.exists(path):
        return {}
    ts = pd.read_csv(path)
    ts = ts[ts["node"].str.startswith("worker")]
    out = {}
    for m in ("net_tx_MBps", "disk_read_MBps", "disk_write_MBps"):
        v = ts[ts["metric"] == m].groupby("node")["value"].quantile(q)
        out[m + "_p95"] = v.mean() if len(v) else np.nan
    return out


def fig_throughput(main, n_workers=3, cap_MBps=12.5):
    """Disk throughput per storage codec and network throughput per shuffle codec vs dataset size.
    Average = bytes moved per worker / run window; peak = 95th percentile of the 1 s samples."""
    m = main.copy()
    # per-worker averages over the run window, the same definition as the workload characterisation (figure 01)
    m["disk_tp"] = m["disk_MBps"]
    m["net_tp"] = m["net_MBps"]
    p95 = pd.DataFrame([{"run_id": r, **run_percentiles(r)} for r in m["run_id"]])
    m = m.merge(p95, on="run_id", how="left")
    m["disk_p95"] = m["disk_read_MBps_p95"] + m["disk_write_MBps_p95"]
    m["net_p95"] = m["net_tx_MBps_p95"]
    t = m.groupby(["app", "size_gb", "storage", "shuffle"])[["disk_tp", "net_tp", "disk_p95", "net_p95"]].mean()
    t.to_csv(os.path.join(TAB, "throughput_summary.csv"), float_format="%.2f")
    print("  table throughput_summary.csv")
    fig, axes = plt.subplots(2, 3, figsize=(12, 6.8))
    for c, app in enumerate(APPS):
        a = m[m["app"] == app]
        ax = axes[0][c]
        line_panel(ax, a[a["shuffle"] == "off"], "storage", STORAGE_ORDER, "disk_tp")
        ax.set_title(f"{APP_LABEL[app]}\nstorage codec, shuffle uncompressed", fontsize=9)
        ax.set_ylabel("Disk read + write per worker (MB/s)")
        ax = axes[1][c]
        sub = a[a["storage"] == "none"]
        line_panel(ax, sub, "shuffle", SHUFFLE_ORDER, "net_tp")
        g = sub.groupby(["shuffle", "size_gb"])["net_p95"].mean()
        for codec in SHUFFLE_ORDER:
            if codec in g.index.get_level_values(0):
                d = g.loc[codec]
                ax.plot(d.index, d.values, color=CODEC_COLOR[codec], lw=1, ls="--")
        ax.axhline(cap_MBps, color=MUTED, lw=1, ls=":")
        ax.annotate("cap 12.5 MB/s", (0.1, cap_MBps), xytext=(0, 3), textcoords="offset points", fontsize=7,
                    color=MUTED)
        ax.set_ylim(0, cap_MBps * 1.25)
        ax.set_title(f"{APP_LABEL[app]}\nshuffle codec, storage uncompressed", fontsize=9)
        ax.set_ylabel("Network sent per worker (MB/s)")
    axes[0][2].legend(title="storage", fontsize=8, title_fontsize=8, loc="upper left", bbox_to_anchor=(1.02, 1))
    h, l = axes[1][2].get_legend_handles_labels()
    h += [plt.Line2D([], [], color=INK2, lw=2), plt.Line2D([], [], color=INK2, lw=1, ls="--")]
    l += ["average", "peak (95th pct.)"]
    axes[1][2].legend(h, l, title="shuffle", fontsize=8, title_fontsize=8, loc="upper left",
                      bbox_to_anchor=(1.02, 1))
    fig.suptitle("Throughput per worker, average over the run (mean ± sd of 3 runs; dashed: 95th percentile of 1 s samples)",
                 fontsize=10, fontweight="bold")
    save(fig, "14_throughput_vs_size")


def summary_tables(df):
    main = df[df["suite"] == "main"]
    if main.empty:
        return
    cols = ["time_s", "cpu_s", "exec_cpu_s", "disk_bytes", "net_tx", "shuffle_write_bytes", "fetch_wait_s",
            "shuffle_read_stages_s", "cpu_util", "iowait_pct"]
    t = main.groupby(["app", "size_gb", "storage", "shuffle"])[cols].agg(["mean", "std", "count"])
    t.to_csv(os.path.join(TAB, "main_summary.csv"))
    print("  table main_summary.csv")


def main():
    df = load_runs()
    print(f"{len(df)} successful runs loaded from {RES}")
    main_runs = df[df["suite"] == "main"]
    fig_characterization(df)
    fig_timeline(df)
    fig_codec_bench()
    fig_storage_overheads(df)
    if not main_runs.empty:
        sweep_figure(main_runs, "time_s", "Execution time (s)", "06_time_vs_size")
        sweep_figure(main_runs, "cpu_s", "CPU time, 3 workers (s)", "07_cpu_vs_size")
        sweep_figure(main_runs, "disk_bytes", "Disk I/O, 3 workers (GB)", "08_disk_io_vs_size", 1e-9,
                     which=("storage",))
        sweep_figure(main_runs, "net_tx", "Network traffic, 3 workers (GB)", "09_network_vs_size", 1e-9,
                     which=("shuffle",))
        fig_configs(main_runs)
        fig_workload(main_runs)
        fig_shuffle_detail(main_runs)
        fig_throughput(main_runs)
    summary_tables(df)


if __name__ == "__main__":
    main()
