#!/usr/bin/env python3
"""Plotting utilities for multi-hop lateral movement analysis."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import FancyArrowPatch
from tqdm import tqdm

sns.set_theme(style="whitegrid")


@dataclass
class VisualizerConfig:
    """Configuration for MultiHopChainVisualizer."""

    chains_csv: Path
    predictions_csv: Optional[Path] = None
    method_comparison_csv: Optional[Path] = None
    hop_summary_csv: Optional[Path] = None
    window_results_glob: Optional[str] = None
    results_dir: Path = Path("thesis_results")
    figures_dir: Path = Path("thesis_figures")
    cache_plots: bool = False


class MultiHopChainVisualizer:
    """Generates diagnostic plots for thesis multi-hop chain analysis."""

    def __init__(self, config: VisualizerConfig) -> None:
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self._chains_df: Optional[pd.DataFrame] = None
        self._pred_df: Optional[pd.DataFrame] = None
        self._method_df: Optional[pd.DataFrame] = None
        self._hop_summary_df: Optional[pd.DataFrame] = None
        self.figures_dir = config.figures_dir
        self.figures_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Loading helpers
    # ------------------------------------------------------------------
    def load_chains(self) -> pd.DataFrame:
        if self._chains_df is None:
            self.logger.info("Loading multi-hop chains from %s", self.config.chains_csv)
            self._chains_df = pd.read_csv(self.config.chains_csv)
        return self._chains_df

    def load_predictions(self) -> pd.DataFrame:
        if self.config.predictions_csv is None:
            raise ValueError("predictions_csv not supplied in config")
        if self._pred_df is None:
            self.logger.info("Loading pivot predictions from %s", self.config.predictions_csv)
            self._pred_df = pd.read_csv(self.config.predictions_csv)
            if "recon_time" in self._pred_df.columns:
                self._pred_df["recon_dt"] = pd.to_datetime(self._pred_df["recon_time"], unit="s")
        return self._pred_df

    def load_method_comparison(self) -> pd.DataFrame:
        if self.config.method_comparison_csv is None:
            raise ValueError("method_comparison_csv not supplied in config")
        if self._method_df is None:
            self.logger.info("Loading method comparison from %s", self.config.method_comparison_csv)
            self._method_df = pd.read_csv(self.config.method_comparison_csv)
        return self._method_df

    def load_hop_summary(self) -> pd.DataFrame:
        if self.config.hop_summary_csv is None:
            raise ValueError("hop_summary_csv not supplied in config")
        if self._hop_summary_df is None:
            self.logger.info("Loading hop summary from %s", self.config.hop_summary_csv)
            self._hop_summary_df = pd.read_csv(self.config.hop_summary_csv)
        return self._hop_summary_df

    # ------------------------------------------------------------------
    # Core graph construction
    # ------------------------------------------------------------------
    def build_multi_hop_graph(
        self,
        level: str = "subnet",
        include_weights: bool = True,
    ) -> Tuple[nx.DiGraph, pd.DataFrame]:
        chains = self.load_chains()
        hop_cols = [col for col in chains.columns if col.startswith("hop") and col.endswith(f"_{level}")]
        if len(hop_cols) < 2:
            raise ValueError(f"Expected at least two hop columns for level '{level}'")
        hop_cols.sort()

        all_edges: List[Tuple[str, str]] = []
        for idx in range(len(hop_cols) - 1):
            pairs = chains[[hop_cols[idx], hop_cols[idx + 1]]].dropna()
            all_edges.extend(pairs.itertuples(index=False, name=None))

        if not all_edges:
            raise RuntimeError("No edges derived from multi-hop chains; check input CSV")

        edge_df = (
            pd.DataFrame(all_edges, columns=["source", "target"])
            .value_counts()
            .reset_index(name="weight")
        )

        G = nx.DiGraph()
        for _, row in edge_df.iterrows():
            if include_weights:
                G.add_edge(row["source"], row["target"], weight=row["weight"])
            else:
                G.add_edge(row["source"], row["target"])
        return G, edge_df

    def _malicious_nodes(self, level: str = "subnet") -> Dict[str, bool]:
        try:
            preds = self.load_predictions()
        except ValueError:
            return {}
        key = "subnet" if level == "subnet" else "pivot_ip"
        if key not in preds.columns:
            if level == "ip" and "pivot_ips" in preds.columns:
                exploded = preds["pivot_ips"].fillna("").str.split(";")
                ips = (
                    exploded.explode()
                    .str.strip()
                    .loc[lambda s: s.ne("")]
                    .to_frame("ip")
                )
                flagged = ips["ip"].value_counts().gt(0)
                return flagged.to_dict()
            return {}
        flagged = preds.loc[preds["became_pivot"].astype(bool), key].value_counts().gt(0)
        return flagged.to_dict()

    # ------------------------------------------------------------------
    # Plotting helpers
    # ------------------------------------------------------------------
    def plot_multi_hop_graph(
        self,
        level: str = "subnet",
        layout: str = "spring",
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        G, edge_df = self.build_multi_hop_graph(level=level)
        malicious = self._malicious_nodes(level)
        pos: Dict[str, Tuple[float, float]]
        if layout == "circular":
            pos = nx.circular_layout(G)
        elif layout == "kamada_kawai":
            pos = nx.kamada_kawai_layout(G)
        else:
            pos = nx.spring_layout(G, seed=42)

        weights = [max(1.0, G[u][v].get("weight", 1) / edge_df["weight"].max() * 5.0) for u, v in G.edges]
        node_colors = ["#d62728" if malicious.get(node, False) else "#1f77b4" for node in G.nodes]

        fig, ax = plt.subplots(figsize=(10, 7))
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=600, ax=ax)
        nx.draw_networkx_labels(G, pos, font_size=8, ax=ax)
        nx.draw_networkx_edges(
            G,
            pos,
            width=weights,
            edge_color="#ff7f0e",
            arrows=True,
            arrowstyle="-|>",
            arrowsize=15,
            ax=ax,
        )
        ax.set_title(f"Multi-hop graph ({level})")
        ax.axis("off")

        handles = [plt.Line2D([0], [0], marker="o", color="w", label="Pivot", markerfacecolor="#d62728", markersize=10),
                   plt.Line2D([0], [0], marker="o", color="w", label="Non-pivot", markerfacecolor="#1f77b4", markersize=10)]
        ax.legend(handles=handles, loc="lower left")

        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved multi-hop graph to %s", output_path)
        return fig

    def plot_hop_transition_grid(
        self,
        level: str = "subnet",
        max_edges: int = 30,
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        chains = self.load_chains()
        hop_cols = [col for col in chains.columns if col.startswith("hop") and col.endswith(f"_{level}")]
        hop_cols.sort()
        num_pairs = max(len(hop_cols) - 1, 1)
        cols = min(3, num_pairs)
        rows = int(np.ceil(num_pairs / cols))
        fig, axes = plt.subplots(rows, cols, figsize=(cols * 5, rows * 4), squeeze=False)

        malicious = self._malicious_nodes(level)
        for idx in range(num_pairs):
            ax = axes[idx // cols, idx % cols]
            source_col, target_col = hop_cols[idx], hop_cols[idx + 1]
            pairs = chains[[source_col, target_col]].dropna()
            counts = pairs.value_counts().reset_index(name="weight").head(max_edges)
            if counts.empty:
                ax.set_title(f"{source_col} → {target_col}\n(no data)")
                ax.axis("off")
                continue
            G = nx.DiGraph()
            for _, row in counts.iterrows():
                G.add_edge(row[source_col], row[target_col], weight=row["weight"])
            pos = nx.spring_layout(G, seed=idx)
            node_colors = ["#d62728" if malicious.get(node, False) else "#1f77b4" for node in G.nodes]
            nx.draw_networkx(G, pos, node_color=node_colors, node_size=500, width=2, arrows=True, ax=ax, font_size=8)
            ax.set_title(f"{source_col.replace('_', ' ')} → {target_col.replace('_', ' ')}")
            ax.axis("off")
        for extra in range(num_pairs, rows * cols):
            axes[extra // cols, extra % cols].axis("off")

        fig.suptitle(f"Hop transitions ({level})", fontsize=14)
        fig.tight_layout(rect=[0, 0, 1, 0.95])
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved hop transition grid to %s", output_path)
        return fig

    def plot_hop_transition_heatmap(
        self,
        level: str = "subnet",
        hop_index: int = 0,
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        chains = self.load_chains()
        hop_cols = [col for col in chains.columns if col.startswith("hop") and col.endswith(f"_{level}")]
        hop_cols.sort()
        if hop_index >= len(hop_cols) - 1:
            raise ValueError("hop_index exceeds available transitions")
        df = (
            chains[[hop_cols[hop_index], hop_cols[hop_index + 1]]]
            .dropna()
            .value_counts()
            .rename("weight")
            .reset_index()
        )
        pivot = df.pivot_table(index=hop_cols[hop_index], columns=hop_cols[hop_index + 1], values="weight", fill_value=0)
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(pivot, annot=False, cmap="Blues", ax=ax)
        ax.set_title(f"Hop {hop_index} → {hop_index + 1} transition heatmap ({level})")
        ax.set_xlabel("Target")
        ax.set_ylabel("Source")
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved transition heatmap to %s", output_path)
        return fig

    def plot_temporal_ribbon(
        self,
        top_n: int = 5,
        resample: str = "12H",
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        preds = self.load_predictions().copy()
        if "subnet" not in preds.columns:
            raise ValueError("Predictions file missing 'subnet' column")
        if "recon_dt" not in preds.columns:
            if "recon_time" not in preds.columns:
                raise ValueError("Predictions file missing 'recon_time' column for temporal plots")
            preds["recon_dt"] = pd.to_datetime(preds["recon_time"], unit="s")
        preds["became_pivot"] = preds["became_pivot"].astype(bool)
        preds["event_type"] = np.where(preds["became_pivot"], "Pivot", "Non-pivot")

        top_subnets = (
            preds.groupby("subnet")["became_pivot"].sum().nlargest(top_n).index.tolist()
        )
        if not top_subnets:
            raise RuntimeError("Unable to identify top subnets for temporal ribbon plot")

        trimmed = preds[preds["subnet"].isin(top_subnets)].copy()
        counts = (
            trimmed.set_index("recon_dt")
            .groupby(["subnet", "event_type"])
            .resample(resample)
            .size()
            .rename("count")
            .reset_index()
        )
        if counts.empty:
            raise RuntimeError("No temporal activity available after resampling; adjust parameters")

        fig, axes = plt.subplots(len(top_subnets), 1, figsize=(11, 3 * len(top_subnets)), sharex=True)
        if len(top_subnets) == 1:
            axes = [axes]
        for ax, subnet in zip(axes, top_subnets):
            subset = counts[counts["subnet"] == subnet].pivot_table(
                index="recon_dt", columns="event_type", values="count", fill_value=0
            ).sort_index()
            pivot_counts = subset.get("Pivot", pd.Series(dtype=float))
            non_counts = subset.get("Non-pivot", pd.Series(dtype=float))
            ax.plot(pivot_counts.index, pivot_counts.values, color="#d62728", label="Pivot")
            ax.plot(non_counts.index, non_counts.values, color="#1f77b4", label="Non-pivot")
            ax.fill_between(pivot_counts.index, pivot_counts.values, color="#d62728", alpha=0.2)
            ax.fill_between(non_counts.index, -non_counts.values, color="#1f77b4", alpha=0.15)
            ax.set_ylabel("Count")
            ax.set_title(f"Subnet {subnet}")
            ax.axhline(0, color="black", linewidth=0.5)
        axes[-1].set_xlabel(f"Recon time (resampled {resample})")
        axes[0].legend(loc="upper right")
        fig.suptitle("Temporal ribbon of pivot vs non-pivot windows", fontsize=14)
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved temporal ribbon plot to %s", output_path)
        return fig

    def plot_similarity_scatter(
        self,
        x_metric: str = "fastrp_similarity",
        y_metric: str = "avg_burst_norm",
        output_path: Optional[Path] = None,
        sample: Optional[int] = 5000,
    ) -> plt.Figure:
        preds = self.load_predictions().copy()
        missing = [col for col in (x_metric, y_metric, "became_pivot") if col not in preds.columns]
        if missing:
            raise ValueError(f"Predictions file missing columns: {missing}")
        data = preds[[x_metric, y_metric, "became_pivot"]].dropna()
        if sample and len(data) > sample:
            data = data.sample(sample, random_state=42)

        fig, ax = plt.subplots(figsize=(8, 6))
        sns.scatterplot(
            data=data,
            x=x_metric,
            y=y_metric,
            hue=data["became_pivot"].astype(bool),
            palette={True: "#d62728", False: "#1f77b4"},
            alpha=0.5,
            ax=ax,
        )
        ax.set_title(f"{x_metric.replace('_', ' ').title()} vs {y_metric.replace('_', ' ').title()}")
        ax.set_xlabel(x_metric.replace("_", " ").title())
        ax.set_ylabel(y_metric.replace("_", " ").title())
        ax.legend(title="Became pivot", loc="best")
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved similarity scatter to %s", output_path)
        return fig

    def plot_cumulative_pivots(
        self,
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        preds = self.load_predictions().sort_values("recon_dt")
        preds["pivot_cumsum"] = preds["became_pivot"].astype(int).cumsum()
        preds["total_cumsum"] = np.arange(1, len(preds) + 1)
        preds["pivot_rate"] = preds["pivot_cumsum"] / preds["total_cumsum"]

        fig, ax1 = plt.subplots(figsize=(9, 5))
        ax1.plot(preds["recon_dt"], preds["pivot_cumsum"], color="#d62728", label="Pivot windows")
        ax1.set_ylabel("Cumulative pivots", color="#d62728")
        ax2 = ax1.twinx()
        ax2.plot(preds["recon_dt"], preds["pivot_rate"], color="#1f77b4", label="Pivot rate")
        ax2.set_ylabel("Pivot rate", color="#1f77b4")
        ax1.set_title("Cumulative pivot coverage over time")
        ax1.set_xlabel("Recon time")
        fig.autofmt_xdate()
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved cumulative pivot plot to %s", output_path)
        return fig

    def plot_subnet_chord(
        self,
        level: str = "subnet",
        top_n: int = 12,
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        _, edge_df = self.build_multi_hop_graph(level=level)
        top_edges = edge_df.sort_values("weight", ascending=False).head(top_n)
        if top_edges.empty:
            raise RuntimeError("No edges available for chord plot; consider adjusting top_n")
        nodes = pd.Index(sorted(set(top_edges["source"]).union(set(top_edges["target"]))))
        angles = np.linspace(0, 2 * np.pi, len(nodes), endpoint=False)
        node_pos = {node: angles[idx] for idx, node in enumerate(nodes)}

        fig, ax = plt.subplots(figsize=(8, 8))
        ax.set_axis_off()
        radius = 1.0
        node_xy = {node: np.array([radius * np.cos(angle), radius * np.sin(angle)]) for node, angle in node_pos.items()}

        for node, coord in node_xy.items():
            ax.scatter(*coord, color="#1f77b4", s=80)
            ax.text(coord[0] * 1.1, coord[1] * 1.1, node, ha="center", va="center", fontsize=8)

        max_weight = top_edges["weight"].max()
        for _, row in top_edges.iterrows():
            start_xy = node_xy[row["source"]]
            end_xy = node_xy[row["target"]]
            arc = FancyArrowPatch(
                start_xy,
                end_xy,
                connectionstyle="arc3,rad=0.2",
                color="#ff7f0e",
                linewidth=1 + 4 * (row["weight"] / max_weight),
                arrowstyle="-|>",
                mutation_scale=12,
            )
            ax.add_patch(arc)

        circle = plt.Circle((0, 0), radius, color="lightgrey", fill=False, linestyle="--", linewidth=0.8)
        ax.add_artist(circle)
        ax.set_xlim(-1.3, 1.3)
        ax.set_ylim(-1.3, 1.3)
        ax.set_title(f"Chord view of top {top_n} {level} transitions")
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved subnet chord plot to %s", output_path)
        return fig

    def plot_effect_size_forest(
        self,
        value_column: str = "Cohen's d",
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        methods = self.load_method_comparison()
        if value_column in methods.columns:
            ordered = methods.sort_values(value_column)
            labels = ordered["Method"]
            values = ordered[value_column]
        else:
            preds = self.load_predictions()
            if "became_pivot" not in preds.columns:
                raise ValueError("Predictions file missing 'became_pivot' column for effect size computation")

            metric_map = {
                "FastRP Similarity": "fastrp_similarity",
                "Avg PageRank": "avg_pagerank_norm",
                "Max PageRank": "max_pagerank_norm",
                "Avg Betweenness": "avg_betweenness_norm",
                "Max Betweenness": "max_betweenness_norm",
                "Avg Clustering": "avg_clustering_norm",
                "Avg Velocity": "avg_velocity_norm",
                "Max Velocity": "max_velocity_norm",
                "Avg Burst": "avg_burst_norm",
                "Subnet Size": "subnet_size_norm",
            }

            pivots = preds[preds["became_pivot"].astype(bool)]
            non_pivots = preds[~preds["became_pivot"].astype(bool)]
            results = []

            def _cohen_d(series_a: pd.Series, series_b: pd.Series) -> float:
                a = series_a.dropna().astype(float)
                b = series_b.dropna().astype(float)
                if len(a) < 2 or len(b) < 2:
                    return np.nan
                mean_diff = a.mean() - b.mean()
                var_a = a.var(ddof=1)
                var_b = b.var(ddof=1)
                pooled = np.sqrt(((len(a) - 1) * var_a + (len(b) - 1) * var_b) / max(len(a) + len(b) - 2, 1))
                if pooled == 0:
                    return np.nan
                return mean_diff / pooled

            for label, column in metric_map.items():
                if column not in preds.columns:
                    continue
                effect = _cohen_d(pivots[column], non_pivots[column])
                results.append((label, effect))

            if not results:
                raise ValueError(
                    "No effect size data available; ensure method comparison includes the target column or predictions include metric columns"
                )

            ordered_df = pd.DataFrame(results, columns=["Metric", "EffectSize"]).dropna()
            ordered_df.sort_values("EffectSize", inplace=True)
            labels = ordered_df["Metric"]
            values = ordered_df["EffectSize"]

        fig, ax = plt.subplots(figsize=(9, max(4, 0.45 * len(labels))))
        ax.hlines(labels, xmin=0, xmax=values, color="#1f77b4")
        ax.plot(values, labels, "o", color="#d62728")
        ax.axvline(0, color="black", linestyle="--", linewidth=1)
        ax.set_xlabel(value_column)
        ax.set_title("Effect size comparison")
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved effect size forest to %s", output_path)
        return fig

    def plot_window_auc_heatmap(
        self,
        focus_method: str = "FastRP Embedding",
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        if not self.config.window_results_glob:
            raise ValueError("window_results_glob not supplied in config")
        rows = []
        base_dir = self.config.results_dir
        for path_str in tqdm(sorted(base_dir.glob(self.config.window_results_glob)), desc="Window files"):
            parts = path_str.stem.split("_")
            if len(parts) < 4:
                continue
            hist_hours, det_hours = int(parts[2]), int(parts[3])
            df = pd.read_csv(path_str)
            subset = df[df["Method"] == focus_method]
            if subset.empty:
                continue
            rows.append({
                "historical": hist_hours,
                "detection": det_hours,
                "auc_roc": subset.iloc[0]["AUC-ROC"]
            })
        if not rows:
            raise RuntimeError("No window optimization results matched the pattern")
        pivot = pd.DataFrame(rows).pivot_table(index="historical", columns="detection", values="auc_roc")
        fig, ax = plt.subplots(figsize=(6, 5))
        sns.heatmap(pivot, annot=True, fmt=".3f", cmap="viridis", ax=ax)
        ax.set_title(f"AUC-ROC heatmap ({focus_method})")
        ax.set_xlabel("Detection window (hours)")
        ax.set_ylabel("Historical window (hours)")
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved window heatmap to %s", output_path)
        return fig

    def plot_degree_distributions(
        self,
        column: str = "subnet_size",
        output_path: Optional[Path] = None,
    ) -> plt.Figure:
        preds = self.load_predictions()
        if column not in preds.columns:
            raise ValueError(f"Predictions missing '{column}' column")
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.kdeplot(data=preds, x=column, hue=preds["became_pivot"].astype(bool), fill=True, common_norm=False, ax=ax)
        ax.set_title(f"Distribution of {column}")
        ax.set_xlabel(column.replace("_", " ").title())
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            self.logger.info("Saved degree distribution to %s", output_path)
        return fig

    # ------------------------------------------------------------------
    # Batch export convenience
    # ------------------------------------------------------------------
    def export_all_plots(self, output_pdf: Optional[Path] = None) -> None:
        figures: List[Tuple[str, plt.Figure]] = []
        figures.append(("multi_hop_graph", self.plot_multi_hop_graph()))
        figures.append(("hop_transition_grid", self.plot_hop_transition_grid()))
        figures.append(("hop_heatmap", self.plot_hop_transition_heatmap()))
        try:
            figures.append(("temporal_ribbon", self.plot_temporal_ribbon()))
            figures.append(("similarity_scatter", self.plot_similarity_scatter()))
            figures.append(("cumulative_pivots", self.plot_cumulative_pivots()))
            figures.append(("degree_distribution", self.plot_degree_distributions()))
        except ValueError as exc:
            self.logger.warning("Skipping prediction-based plots: %s", exc)
        try:
            figures.append(("effect_size_forest", self.plot_effect_size_forest()))
        except ValueError as exc:
            self.logger.warning("Skipping effect-size plot: %s", exc)
        try:
            figures.append(("window_auc_heatmap", self.plot_window_auc_heatmap()))
        except (ValueError, RuntimeError) as exc:
            self.logger.warning("Skipping window heatmap: %s", exc)
        figures.append(("subnet_chord", self.plot_subnet_chord()))

        if output_pdf:
            output_pdf = Path(output_pdf)
            output_pdf.parent.mkdir(parents=True, exist_ok=True)
            with PdfPages(output_pdf) as pdf:
                for name, fig in figures:
                    pdf.savefig(fig)
                    plt.close(fig)
            self.logger.info("Exported %d plots to %s", len(figures), output_pdf)
        elif not self.config.cache_plots:
            for _, fig in figures:
                plt.close(fig)


__all__ = ["VisualizerConfig", "MultiHopChainVisualizer"]
