"""Statistics, cost model, memory, and the additional experiments (report section 10).

Usage: python analysis/extra_analysis.py [results_dir]
Writes results/tables/{significance,cost_model,memory,fastnet,incompressible,incompressible_sizes}.csv and
results/figures/13_fast_network.png and 15_incompressible.png (when those runs exist) and prints a summary.
"""
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy import stats  # noqa: E402

RES = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")
TAB = os.path.join(RES, "tables")
FIG = os.path.join(RES, "figures")
APP = {"sort": "Sort", "mixed": "Join", "cpu": "Hash"}


def load():
    rows = []
    with open(os.path.join(RES, "runs.jsonl")) as f:
        for line in f:
            r = json.loads(line)
            if r.get("status") != "ok" or r.get("suite") not in ("main", "fastnet", "random"):
                continue
            sp, w = r.get("spark") or {}, (r.get("system") or {}).get("workers") or {}
            nodes = (r.get("system") or {}).get("nodes") or {}
            wk = [n for n in nodes if n.startswith("worker") and (r.get("suite") not in ("fastnet", "random") or n != "worker3")]
            rows.append({"suite": r["suite"], "net": r.get("net", "cap"), "app": r["app"], "size": r["size_gb"],
                         "storage": r["storage"], "shuffle": r["shuffle"], "rep": r["rep"],
                         "t": sp.get("jobs_s"), "exec_cpu": sp.get("executor_cpu_s"),
                         "gc_share": sp.get("gc_s", 0) / max(sp.get("executor_run_s", 1), 1),
                         "net_gb": w.get("net_tx_bytes", 0) / 1e9,
                         "disk_gb": (w.get("disk_read_bytes", 0) + w.get("disk_write_bytes", 0)) / 1e9,
                         "cpu_busy": w.get("cpu_busy_s"), "iowait": w.get("cpu_iowait_s"),
                         "mem_max_gb": max([nodes[n].get("mem_used_max_bytes", 0) for n in wk] or [0]) / 1e9})
    d = pd.DataFrame(rows)
    d["cfg"] = d["storage"] + "/" + d["shuffle"]
    return d


def ratio_ci(x, y):
    """mean(x)/mean(y) with a 95 % CI (Fieller-free approximation via Welch on the difference) and Welch p."""
    x, y = np.asarray(x, float), np.asarray(y, float)
    mx, my = x.mean(), y.mean()
    vx, vy = x.var(ddof=1) / len(x), y.var(ddof=1) / len(y)
    se = np.sqrt(vx + vy)
    df = (vx + vy) ** 2 / (vx ** 2 / (len(x) - 1) + vy ** 2 / (len(y) - 1)) if se > 0 else 1e9
    q = stats.t.ppf(0.975, df)
    p = stats.ttest_ind(x, y, equal_var=False).pvalue if se > 0 else 0.0
    return mx / my, (mx - mx + (mx - my) - q * se) / my + 1, ((mx - my) + q * se) / my + 1, p


def significance(d):
    m = d[d.suite == "main"]
    out = []
    for (app, size), g in m.groupby(["app", "size"]):
        base = g[g.cfg == "none/off"].t
        for cfg, h in g.groupby("cfg"):
            if cfg == "none/off":
                continue
            r, lo, hi, p = ratio_ci(h.t, base)
            out.append({"app": APP[app], "size_gb": size, "config": cfg, "rel": r, "ci95_low": lo, "ci95_high": hi,
                        "p_welch": p, "verdict": "faster" if hi < 1 else "slower" if lo > 1 else "not significant"})
    s = pd.DataFrame(out)
    # multiple comparisons (96 tests): Holm (family-wise error) and Benjamini-Hochberg (false discovery rate)
    p, m = s.p_welch.values, len(s)
    o = np.argsort(p)
    holm = np.minimum(1, np.maximum.accumulate((m - np.arange(m)) * p[o]))
    bh = np.minimum(1, np.minimum.accumulate((m / np.arange(m, 0, -1) * p[o][::-1]))[::-1])
    s.loc[s.index[o], "p_holm"] = holm
    s.loc[s.index[o], "q_bh"] = bh
    s.to_csv(os.path.join(TAB, "significance.csv"), index=False, float_format="%.6g")  # full p precision
    print("== significance (95 % CI of time / C0, Welch, n = 3 vs 3)")
    print(s.groupby(["app", "verdict"]).size().unstack(fill_value=0))
    f = s[s.verdict == "faster"]
    print("faster and surviving correction (Holm p < 0.05, BH q < 0.05):")
    print(f.groupby("app").agg(faster=("rel", "size"), holm=("p_holm", lambda v: int((v < 0.05).sum())),
                               bh=("q_bh", lambda v: int((v < 0.05).sum()))))
    print(s[s.verdict == "slower"][["app", "size_gb", "config", "rel", "ci95_low", "ci95_high"]].round(3).to_string())
    return s


def cost_model(d):
    """T = a + b * network GB + c * executor CPU s, fitted per application on the 108 main runs."""
    m = d[d.suite == "main"]
    out = []
    print("== cost model  T = a + b*net_GB + c*exec_cpu_s  (per application)")
    for app, g in m.groupby("app"):
        X = np.column_stack([np.ones(len(g)), g.net_gb, g.exec_cpu])
        coef, *_ = np.linalg.lstsq(X, g.t.values, rcond=None)
        pred = X @ coef
        r2 = 1 - ((g.t - pred) ** 2).sum() / ((g.t - g.t.mean()) ** 2).sum()
        mape = (abs(g.t - pred) / g.t).mean()
        out.append({"app": APP[app], "a_s": coef[0], "b_s_per_GB": coef[1], "c_s_per_cpu_s": coef[2],
                    "eff_bandwidth_MBps": 1000 / coef[1] if coef[1] > 0 else np.nan,
                    "eff_cores": 1 / coef[2] if coef[2] > 0 else np.nan, "R2": r2, "MAPE": mape})
    c = pd.DataFrame(out)
    c.to_csv(os.path.join(TAB, "cost_model.csv"), index=False, float_format="%.6g")
    print(c.round(3).to_string())
    return c


def memory(d):
    m = d[(d.suite == "main") & (d["size"] == 4.0)]
    t = m.groupby(["app", "cfg"]).agg(mem_max_gb=("mem_max_gb", "mean"), gc_share=("gc_share", "mean")).reset_index()
    t["app"] = t.app.map(APP)
    t.to_csv(os.path.join(TAB, "memory.csv"), index=False, float_format="%.3f")
    print("== memory at 4 GB: max used on the busiest worker (GB), GC share of task time")
    print(t.pivot(index="cfg", columns="app", values="mem_max_gb").round(2).to_string())
    print("GC share range:", round(t.gc_share.min(), 3), "-", round(t.gc_share.max(), 3))


FAST_NAMES = {"none/off": "C0", "zstd/off": "C1", "none/zstd": "C2", "zstd/zstd": "C3"}
RAND_NAMES = dict(FAST_NAMES)
CFG_LABEL = {"C0": "C0 none/off", "C1": "C1 zstd/off", "C2": "C2 none/zstd", "C3": "C3 zstd/zstd"}


def regimes(d, suite, names, csv, header):
    """Per network regime, application and configuration: time relative to C0 with a 95 % CI, and resources."""
    f = d[d.suite == suite]
    if f.empty:
        return None
    f = f.assign(C=f.cfg.map(names))
    out = []
    for (net, app), g in f.groupby(["net", "app"]):
        base = g[g.C == "C0"]
        for c, h in g.groupby("C"):
            r, lo, hi, p = ratio_ci(h.t, base.t) if c != "C0" else (1, np.nan, np.nan, np.nan)
            out.append({"net": net, "app": APP[app], "config": c, "t_mean": h.t.mean(), "t_sd": h.t.std(),
                        "rel_C0": r, "ci95_low": lo, "ci95_high": hi, "p_welch": p, "net_gb": h.net_gb.mean(),
                        "exec_cpu_s": h.exec_cpu.mean(), "cpu_busy_s": h.cpu_busy.mean(), "disk_gb": h.disk_gb.mean(),
                        "iowait_s": h.iowait.mean(), "iowait_max_s": h.iowait.max()})
    x = pd.DataFrame(out)
    x.to_csv(os.path.join(TAB, csv), index=False, float_format="%.4f")
    print(header)
    print(x.round(3).to_string())
    return x


def fastnet(d):
    x = regimes(d, "fastnet", FAST_NAMES, "fastnet.csv", "== fast network experiment (2 workers, 2 GB, replication 1)")
    if x is not None:
        fig_regimes(x, ["C0", "C1", "C2", "C3"], "13_fast_network",
                    "Same 2 workers, 2 GB, replication 1: C0 none | C1 storage zstd | C2 shuffle zstd | C3 both"
                    "   (labels: time relative to C0)")


def random_data(d):
    """Experiment R: Sort on incompressible data (160 random bytes per row), same 2-worker setup as fastnet."""
    x = regimes(d, "random", RAND_NAMES, "incompressible.csv",
                "== incompressible data (Sort, 2 workers, 2 GB, replication 1)")
    if x is None:
        return
    sizes = {}
    with open(os.path.join(RES, "runs.jsonl")) as f:
        for line in f:
            r = json.loads(line)
            if r.get("suite") == "random-prep" and r.get("status") == "ok" and r["run_id"] != "random-calibrate":
                ar, sp = r.get("app_result") or {}, r.get("spark") or {}
                sizes[r["storage"]] = {"events_bytes": ar.get("events_bytes"), "write_s": sp.get("jobs_s"),
                                       "write_exec_cpu_s": sp.get("executor_cpu_s")}
    if sizes:
        z = pd.DataFrame(sizes).T
        z["ratio"] = z.loc["none", "events_bytes"] / z["events_bytes"] if "none" in z.index else np.nan
        z.to_csv(os.path.join(TAB, "incompressible_sizes.csv"), float_format="%.3f")
        print(z.round(3).to_string())
    fig_regimes(x, ["C0", "C1", "C2", "C3"], "15_incompressible",
                "Sort on nearly incompressible data (160 random bytes per row), 2 workers, 2 GB, replication 1"
                "   (labels: time relative to C0)")


def fig_regimes(x, order, name, suptitle):
    """Execution time per configuration on the same 2 workers, with and without the 100 Mbit/s limit."""
    ink2, base = "#52514e", "#c3c2b7"
    color = {"C0": ink2, "C1": "#2a78d6", "C2": "#eb6834", "C3": "#1baf7a"}
    plt.rcParams.update({
        "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "savefig.facecolor": "#fcfcfb",
        "font.family": "DejaVu Sans", "font.size": 9, "axes.titlesize": 10, "axes.titleweight": "bold",
        "axes.edgecolor": base, "axes.labelcolor": ink2, "xtick.color": "#898781", "ytick.color": "#898781",
        "axes.grid": True, "grid.color": "#e1e0d9", "grid.linewidth": 0.6, "axes.axisbelow": True,
        "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False})
    apps = ["Sort", "Join", "Hash"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), gridspec_kw={"width_ratios": [2, 3]})
    for ax, net, title in ((axes[0], "cap", "100 Mbit/s limit (as in the main study)"),
                           (axes[1], "nocap", "No limit (WireGuard between the desktop VMs)")):
        g = x[x.net == net]
        shown = [a for a in apps if a in set(g.app)]
        cfgs = [c for c in order if c in set(g.config)]
        width = 0.8 / max(len(cfgs), 3)
        for k, c in enumerate(cfgs):
            d = g[g.config == c].set_index("app").reindex(shown)
            xs = np.arange(len(shown)) + (k - (len(cfgs) - 1) / 2) * width
            ax.bar(xs, d.t_mean, width=width, yerr=d.t_sd, color=color[c], label=CFG_LABEL[c], edgecolor="#fcfcfb",
                   linewidth=1.5, error_kw={"ecolor": ink2, "elinewidth": 1, "capsize": 3})
            if c != "C0":
                for xi, (t, sd, r) in zip(xs, zip(d.t_mean, d.t_sd, d.rel_C0)):
                    if not np.isnan(t):
                        ax.annotate(f"{r:.2f}", (xi, t + (sd if not np.isnan(sd) else 0)), xytext=(0, 3),
                                    textcoords="offset points", ha="center", fontsize=8, color=ink2)
        ax.set_xticks(range(len(shown)))
        ax.set_xticklabels(shown)
        ax.set_title(title)
        ax.set_ylabel("Execution time (s)")
    axes[1].legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
    fig.suptitle(suptitle, fontsize=10, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, name + ".png"), dpi=160)
    plt.close(fig)
    print("  figure", name)


if __name__ == "__main__":
    d = load()
    significance(d)
    cost_model(d)
    memory(d)
    fastnet(d)
    random_data(d)
