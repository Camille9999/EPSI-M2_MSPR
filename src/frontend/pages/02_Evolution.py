"""Page 2 — Évolution temporelle des métriques entre runs."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from loader import build_registry_table

st.set_page_config(page_title="Évolution des métriques", page_icon="📈", layout="wide")
st.title("📈 Évolution temporelle des métriques")

df = build_registry_table()
if df.empty:
    st.warning("Aucun run disponible.")
    st.stop()

show_trend = st.sidebar.checkbox("Afficher la ligne de tendance", value=True)

df_h = (
    df
    .dropna(subset=["trained_at"])
    .sort_values("trained_at")
    .reset_index(drop=True)
)

if len(df_h) < 2:
    st.info(
        "Il faut au moins 2 runs pour tracer une évolution. "
        "Lance `train_sarima.py` plusieurs fois."
    )
    st.stop()

METRICS = [
    ("MAE (MW)", "#2ca02c"),
    ("RMSE (MW)", "#9467bd"),
    ("MAPE (%)", "#d62728"),
]

# ── Graphiques par métrique (tabs) ────────────────────────────────────────────
tabs = st.tabs([m for m, _ in METRICS])

for tab, (metric, color) in zip(tabs, METRICS):
    with tab:
        df_plot = df_h.dropna(subset=[metric])
        if df_plot.empty:
            st.info(f"Aucune valeur de {metric} disponible.")
            continue

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df_plot["trained_at"],
                y=df_plot[metric],
                mode="lines+markers",
                name=metric,
                line=dict(color=color, width=2),
                marker=dict(size=9),
                text=df_plot["run_id"],
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    + metric
                    + ": %{y:.2f}<br>"
                    "Date: %{x}<extra></extra>"
                ),
            )
        )

        if show_trend and len(df_plot) >= 3:
            x_num = (df_plot["trained_at"] - df_plot["trained_at"].min()).dt.days.values
            coeffs = np.polyfit(x_num, df_plot[metric].values, 1)
            trend = np.polyval(coeffs, x_num)
            direction = "↗ hausse" if coeffs[0] > 0 else "↘ baisse"
            fig.add_trace(
                go.Scatter(
                    x=df_plot["trained_at"],
                    y=trend,
                    mode="lines",
                    name=f"Tendance ({direction})",
                    line=dict(color=color, width=1.5, dash="dot"),
                    opacity=0.55,
                )
            )

        fig.update_layout(
            title=f"Évolution de la {metric}",
            xaxis_title="Date d'entraînement",
            yaxis_title=metric,
            hovermode="x unified",
        )
        st.plotly_chart(fig, width="stretch")

        # Delta entre avant-dernier et dernier run
        prev_val = df_plot[metric].iloc[-2]
        last_val = df_plot[metric].iloc[-1]
        delta = last_val - prev_val
        pct_delta = delta / prev_val * 100 if prev_val else 0
        sign = "+" if delta > 0 else ""
        col1, col2, col3 = st.columns(3)
        col1.metric("Run précédent", f"{prev_val:.2f}")
        col2.metric(
            "Dernier run",
            f"{last_val:.2f}",
            delta=f"{sign}{delta:.2f}",
            delta_color="inverse",
        )
        col3.metric(
            "Variation",
            f"{sign}{pct_delta:.1f} %",
            delta=f"{sign}{pct_delta:.1f} %",
            delta_color="inverse",
        )

st.divider()

# ── Tableau de synthèse ────────────────────────────────────────────────────────
st.subheader("Tableau de synthèse")
display_df = df_h.copy()
display_df["trained_at"] = display_df["trained_at"].dt.strftime("%Y-%m-%d %H:%M")
st.dataframe(
    display_df[["run_id", "trained_at", "MAE (MW)", "RMSE (MW)", "MAPE (%)", "AIC", "n_training_days"]],
    width="stretch",
    hide_index=True,
)
