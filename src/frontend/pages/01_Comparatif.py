"""Page 1 — Comparatif des runs d'entraînement."""
from __future__ import annotations

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from loader import build_registry_table

st.set_page_config(page_title="Comparatif des runs", page_icon="📊", layout="wide")
st.title("📊 Comparatif des runs")

df = build_registry_table()
if df.empty:
    st.warning("Aucun run disponible. Lance d'abord `train_sarima.py`.")
    st.stop()

# ── Tableau complet ─────────────────────────────────────────────────
st.subheader("Tableau des métriques")
display_df = df.copy()
if "trained_at" in display_df.columns:
    display_df["trained_at"] = display_df["trained_at"].dt.strftime("%Y-%m-%d %H:%M")

st.dataframe(
    display_df[
        [
            "run_id",
            "trained_at",
            "MAE (MW)",
            "RMSE (MW)",
            "MAPE (%)",
            "AIC",
            "BIC",
            "n_training_days",
            "order",
            "seasonal_order",
        ]
    ].sort_values("trained_at"),
    width="stretch",
    hide_index=True,
)
st.download_button(
    "⬇ Télécharger CSV",
    data=df.to_csv(index=False),
    file_name="sarima_runs.csv",
    mime="text/csv",
)

st.divider()

# ── Graphique en barres pour une métrique ─────────────────────────────────────
metric = st.selectbox("Métrique à comparer", ["MAE (MW)", "RMSE (MW)", "MAPE (%)"])
df_plot = df.dropna(subset=[metric]).sort_values("trained_at")

if df_plot.empty:
    st.info(f"Aucune valeur de {metric} disponible.")
else:
    df_plot = df_plot.copy()
    df_plot["label"] = df_plot["trained_at"].dt.strftime("%Y-%m")
    fig = px.bar(
        df_plot,
        x="label",
        y=metric,
        color=metric,
        color_continuous_scale="RdYlGn_r",
        text=df_plot[metric].round(2),
        title=f"Comparatif {metric}",
        labels={"label": "Période"},
    )
    fig.update_traces(texttemplate="%{text}", textposition="outside")
    fig.update_layout(xaxis_tickangle=-30, coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

    col1, col2 = st.columns(2)
    best_idx = df_plot[metric].idxmin()
    worst_idx = df_plot[metric].idxmax()
    col1.success(
        f"✅ Meilleur run : **{df_plot.loc[best_idx, 'run_id']}** "
        f"— {df_plot.loc[best_idx, metric]:.2f}"
    )
    col2.error(
        f"⚠️ Pire run : **{df_plot.loc[worst_idx, 'run_id']}** "
        f"— {df_plot.loc[worst_idx, metric]:.2f}"
    )

st.divider()

# ── Comparaison multi-métriques normalisée ────────────────────────────────────
st.subheader("Comparaison multi-métriques (normalisée par le minimum)")
df_norm = df[["run_id", "trained_at", "MAE (MW)", "RMSE (MW)"]].dropna().copy()
df_norm = df_norm.sort_values("trained_at", ascending=False).reset_index(drop=True)
df_norm["label"] = df_norm["trained_at"].dt.strftime("%Y-%m")
for col in ["MAE (MW)", "RMSE (MW)"]:
    min_val = df_norm[col].min()
    if min_val > 0:
        df_norm[col] = df_norm[col] / min_val

df_melted = df_norm.melt(id_vars="label", value_vars=["MAE (MW)", "RMSE (MW)"], var_name="Métrique", value_name="Valeur normalisée")
fig2 = px.bar(
    df_melted,
    x="label",
    y="Valeur normalisée",
    color="Métrique",
    barmode="group",
    title="MAE et RMSE normalisées (1.0 = meilleur run)",
    labels={"label": "Période"},
)
fig2.add_hline(y=1.0, line_dash="dash", line_color="green", annotation_text="Optimal")
fig2.update_layout(xaxis_tickangle=-30)
st.plotly_chart(fig2, width="stretch")
