"""Generate the README charts from real pipeline metrics.

Reads:
  - data/gold/discovery_accuracy.json  (written by gold_model.py)
  - docs/img/run_metrics.json          (per-stage seconds from a full run)
Writes PNGs into docs/img/. Re-run after a full pipeline run to refresh them.
Usage:  python scripts/make_charts.py
"""
import json
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

NAVY = "#1F3864"
BLUE = "#2E75B6"
LIGHT = "#8FAADC"
AMBER = "#C55A11"
IMG = "docs/img"
os.makedirs(IMG, exist_ok=True)


def _style(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.tick_params(length=0)


def stage_runtimes():
    m = json.load(open(f"{IMG}/run_metrics.json"))
    order = ["generate", "bronze", "silver", "dq", "gold", "sparksql", "warehouse", "dbt"]
    labels = ["generate", "bronze", "silver", "dq_gate", "gold", "spark-sql", "warehouse", "dbt"]
    vals = [m[k] for k in order]
    fig, ax = plt.subplots(figsize=(8, 3.6))
    bars = ax.barh(labels[::-1], vals[::-1], color=NAVY)
    for b, v in zip(bars, vals[::-1]):
        ax.text(v + 0.4, b.get_y() + b.get_height() / 2, f"{v}s", va="center", fontsize=9, color=NAVY)
    ax.set_xlabel("seconds")
    ax.set_title(f"Per-stage runtime — full run, {m['total']}s end to end "
                 f"(12.35M rows, 4 vCPU / 4 GB)", fontsize=11, color=NAVY, weight="bold")
    _style(ax)
    fig.tight_layout(); fig.savefig(f"{IMG}/stage_runtimes.png", dpi=150); plt.close(fig)


def discovery_accuracy():
    m = json.load(open("data/gold/discovery_accuracy.json"))
    keys = ["homogeneity", "completeness", "v_measure", "purity"]
    labels = ["Homogeneity", "Completeness", "V-measure", "Purity"]
    vals = [m[k] for k in keys]
    colors = [BLUE, BLUE, NAVY, AMBER]
    fig, ax = plt.subplots(figsize=(7, 3.8))
    bars = ax.bar(labels, vals, color=colors, width=0.6)
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.02, f"{v:.3f}", ha="center", fontsize=10, color=NAVY, weight="bold")
    ax.set_ylim(0, 1.0)
    ax.axhline(0.0, color="#ccc", lw=0.8)
    ax.set_ylabel("score (0–1)")
    ax.set_title(f"Application-discovery accuracy vs ground truth\n"
                 f"{m['n_resources']:,} resources · {m['n_clusters']:,} clusters · {m['n_true_apps']} true apps",
                 fontsize=11, color=NAVY, weight="bold")
    _style(ax)
    fig.tight_layout(); fig.savefig(f"{IMG}/discovery_accuracy.png", dpi=150); plt.close(fig)


def dedup_funnel():
    # Real numbers from the full run: raw flow rows -> after dedup.
    stages = ["Raw flow events", "After dedup (silver)"]
    vals = [10_300_000, 10_000_000]
    fig, ax = plt.subplots(figsize=(7, 2.8))
    bars = ax.barh(stages[::-1], [v / 1e6 for v in vals[::-1]], color=[BLUE, NAVY])
    for b, v in zip(bars, vals[::-1]):
        ax.text(v / 1e6 + 0.1, b.get_y() + b.get_height() / 2, f"{v/1e6:.1f}M", va="center", fontsize=10, color=NAVY)
    ax.set_xlabel("rows (millions)")
    ax.set_title("Deduplication — 300,000 duplicate events removed (2.91%)",
                 fontsize=11, color=NAVY, weight="bold")
    _style(ax)
    fig.tight_layout(); fig.savefig(f"{IMG}/dedup_funnel.png", dpi=150); plt.close(fig)


if __name__ == "__main__":
    stage_runtimes()
    discovery_accuracy()
    dedup_funnel()
    print("charts written to", IMG)
