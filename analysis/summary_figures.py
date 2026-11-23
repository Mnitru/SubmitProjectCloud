"""Compact summary figures for the short report and the slides.

    python analysis/summary_figures.py [results_dir]

Writes results/figures/16_relative_vs_size.png (C1-C3 relative to C0 per size) and
results/figures/17_network_regimes.png (C3/C0 with and without the 100 Mbit/s cap).
Inputs: results/runs.jsonl, results/tables/fastnet.csv and incompressible.csv
(run analysis/extra_analysis.py first).
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
FIG, TAB = os.path.join(RES, "figures"), os.path.join(RES, "tables")
INK2, MUTED, GRID, BASE = "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"
plt.rcParams.update({
    "figure.facecolor": "#ffffff", "axes.facecolor": "#ffffff", "savefig.facecolor": "#ffffff",
    "font.family": "DejaVu Sans", "font.size": 9, "axes.titlesize": 10, "axes.titleweight": "bold",
    "axes.edgecolor": BASE, "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False})


def main_runs():
    rows = []
    with open(os.path.join(RES, "runs.jsonl"), encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r.get("suite") == "main" and r.get("status") == "ok":
                rows.append({"app": r["app"], "size": r["size_gb"], "cfg": r["storage"] + "/" + r["shuffle"],
                             "t": r["spark"]["jobs_s"]})
    return pd.DataFrame(rows)


def fig_relative_vs_size(d):
    cfgs = {"C1 storage zstd": ("zstd/off", S1, ":"), "C2 shuffle zstd": ("none/zstd", S2, "--"),
            "C3 both": ("zstd/zstd", S3, "-")}
    apps = [("sort", "Sort (I/O-bound)"), ("mixed", "Join (balanced)"), ("cpu", "Hash (CPU-bound)")]
    g = d.groupby(["app", "size", "cfg"]).t.agg(["mean", "std"])
    fig, axes = plt.subplots(1, 3, figsize=(7.6, 2.5), sharey=True)
    sizes = [0.5, 1, 2, 4]
    for ax, (app, title) in zip(axes, apps):
        base = np.array([g.loc[(app, s, "none/off"), "mean"] for s in sizes])
        ax.axhline(1, color=MUTED, lw=1, ls=(0, (4, 3)))
        ends = []
        for label, (cfg, col, ls) in cfgs.items():
            m = np.array([g.loc[(app, s, cfg), "mean"] for s in sizes]) / base
            e = np.array([g.loc[(app, s, cfg), "std"] for s in sizes]) / base
            ax.errorbar(sizes, m, yerr=e, color=col, ls=ls, marker="o", ms=4, lw=1.8 if cfg == "zstd/zstd" else 1.3,
                        elinewidth=0.8, capsize=2, label=label)
            ends.append([m[-1], m[-1], col, cfg == "zstd/zstd"])
        # end labels, pushed apart so that they never overlap
        ends.sort(key=lambda x: x[0])
        for k in range(1, len(ends)):
            ends[k][1] = max(ends[k][1], ends[k - 1][1] + 0.055)
        for v, y, col, bold in ends:
            ax.annotate(f"{v:.2f}", (4, v), xytext=(4.25, y - 0.015), textcoords="data", fontsize=8, color=col,
                        fontweight="bold" if bold else "normal")
        ax.set_title(title)
        ax.set_xticks(sizes)
        ax.set_xticklabels(["0.5", "1", "2", "4"])
        ax.set_xlim(0.2, 4.9)
        ax.set_xlabel("Dataset size (GB)")
    axes[0].set_ylabel("Time / time of C0")
    axes[0].set_ylim(0.3, 1.12)
    axes[0].annotate("C0 = 1.0", (0.3, 1.0), xytext=(0, 3), textcoords="offset points", fontsize=7.5, color=MUTED)
    h, lab = axes[0].get_legend_handles_labels()
    fig.legend(h, lab, loc="upper center", ncol=3, fontsize=8, bbox_to_anchor=(0.5, 1.02))
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(os.path.join(FIG, "16_relative_vs_size.png"), dpi=200)
    plt.close(fig)
    print("  figure 16_relative_vs_size")


def fig_network_regimes():
    f = pd.read_csv(os.path.join(TAB, "fastnet.csv"))
    r = pd.read_csv(os.path.join(TAB, "incompressible.csv"))
    groups = [("Sort", f, "Sort"), ("Join", f, "Join"), ("Hash", f, "Hash"), ("Sort,\nrandom data", r, "Sort")]
    fig, ax = plt.subplots(figsize=(7.6, 2.6))
    width = 0.36
    for k, (net, col, lab) in enumerate((("cap", S1, "100 Mbit/s cap"), ("nocap", S2, "no limit (≈1.3 Gbit/s used)"))):
        for i, (name, tab, app) in enumerate(groups):
            row = tab[(tab.net == net) & (tab.app == app) & (tab.config == "C3")]
            x = i + (k - 0.5) * width
            if row.empty:
                ax.text(x, 0.05, "not run", rotation=90, ha="center", va="bottom", fontsize=7, color=MUTED)
                continue
            v, lo, hi = row.rel_C0.iloc[0], row.ci95_low.iloc[0], row.ci95_high.iloc[0]
            ax.bar(x, v, width=width, color=col, edgecolor="white", linewidth=1.2, label=lab if i == 0 else None)
            ax.errorbar(x, v, yerr=[[v - lo], [hi - v]], color=INK2, elinewidth=0.9, capsize=3)
            ax.annotate(f"{v:.2f}", (x, hi), xytext=(0, 2), textcoords="offset points", ha="center", fontsize=8,
                        color=INK2)
    ax.axhline(1, color=MUTED, lw=1, ls=(0, (4, 3)))
    ax.set_xticks(range(len(groups)))
    ax.set_xticklabels([g[0] for g in groups])
    ax.set_ylabel("C3 time / C0 time")
    ax.set_ylim(0, 1.6)
    ax.legend(fontsize=8, loc="lower center", ncol=2, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "17_network_regimes.png"), dpi=200)
    plt.close(fig)
    print("  figure 17_network_regimes")


if __name__ == "__main__":
    fig_relative_vs_size(main_runs())
    fig_network_regimes()
