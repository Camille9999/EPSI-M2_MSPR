"""Page 4 — Prévisions récursives : dégradation des performances par horizon."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from loader import list_runs, load_model_object

st.set_page_config(page_title="Prévisions récursives", page_icon="🔄", layout="wide")
st.title("🔄 Prévisions récursives — Dégradation par horizon")
st.markdown(
    "Évalue la dégradation des performances lorsque le modèle est utilisé de façon **récursive** : "
    "à chaque point de départ, les prédictions précédentes alimentent les termes AR pour "
    "prédire des horizons de plus en plus lointains."
)

# ── Sélection ─────────────────────────────────────────────────────────────────
runs = list_runs()
if not runs:
    st.warning("Aucun run disponible.")
    st.stop()

run_id = st.sidebar.selectbox("Run", [r["run_id"] for r in runs])
st.sidebar.divider()
MAX_HORIZON = st.sidebar.slider("Horizon maximal (jours)", 3, 30, 14)
EVAL_STEP = st.sidebar.slider("Pas entre points de départ (jours)", 1, 30, 14)
EVAL_WINDOW = st.sidebar.slider("Fenêtre d'évaluation (jours)", 90, 730, 365)


# ── Calcul (mis en cache) ──────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _compute_recursive(
    _run_id: str, max_horizon: int, eval_step: int, eval_window: int
) -> pd.DataFrame:
    """
    Le préfixe _ sur _run_id évite que Streamlit tente de hasher
    un objet potentiellement complexe — on utilise run_id (str) comme clé.
    """
    model = load_model_object(_run_id)
    if model is None:
        return pd.DataFrame()

    model_dates = pd.DatetimeIndex(model.model.data.dates)
    endog_arr = model.model.endog.flatten()
    n = len(endog_arr)

    eval_start_i = max(0, n - eval_window - max_horizon)
    eval_points = list(range(eval_start_i, n - max_horizon, eval_step))

    records = []
    for start_i in eval_points:
        end_i = start_i + max_horizon
        if end_i >= n:
            break
        try:
            preds = model.predict(start=start_i + 1, end=end_i, dynamic=True)
            for h in range(1, max_horizon + 1):
                target_i = start_i + h
                if target_i >= n or h - 1 >= len(preds):
                    break
                actual_val = float(endog_arr[target_i])
                pred_val = float(preds.iloc[h - 1])
                records.append(
                    {
                        "start_date": model_dates[start_i],
                        "target_date": model_dates[target_i],
                        "horizon": h,
                        "actual": actual_val,
                        "predicted": pred_val,
                        "abs_error": abs(pred_val - actual_val),
                        "error": pred_val - actual_val,
                    }
                )
        except Exception:
            pass
    return pd.DataFrame(records)


params_key = (run_id, MAX_HORIZON, EVAL_STEP, EVAL_WINDOW)

if st.button("▶ Calculer les prévisions récursives", type="primary"):
    with st.spinner(f"Calcul sur {EVAL_WINDOW} jours, {MAX_HORIZON} horizons…"):
        result = _compute_recursive(run_id, MAX_HORIZON, EVAL_STEP, EVAL_WINDOW)
    st.session_state["recursive_df"] = result
    st.session_state["recursive_params"] = params_key

recursive_df: pd.DataFrame = st.session_state.get("recursive_df", pd.DataFrame())
cached_params = st.session_state.get("recursive_params")

if cached_params and cached_params != params_key:
    st.info("⚠️ Les paramètres ont changé. Recalcule pour mettre à jour.")

if recursive_df.empty:
    st.info("Clique sur **Calculer** pour lancer l'évaluation récursive.")
    st.stop()

# ── Métriques par horizon ─────────────────────────────────────────────────────
horizon_stats = []
for hh in range(1, MAX_HORIZON + 1):
    sub = recursive_df[recursive_df["horizon"] == hh]
    if sub.empty:
        continue
    horizon_stats.append(
        {
            "Horizon": hh,
            "MAE (MW)": round(mean_absolute_error(sub["actual"], sub["predicted"]), 1),
            "RMSE (MW)": round(
                float(np.sqrt(mean_squared_error(sub["actual"], sub["predicted"]))), 1
            ),
            "MAPE (%)": round(
                mean_absolute_percentage_error(sub["actual"], sub["predicted"]) * 100, 3
            ),
            "n": len(sub),
        }
    )

hm_df = pd.DataFrame(horizon_stats).set_index("Horizon")

st.subheader("Métriques par horizon")
st.dataframe(hm_df, width="stretch")

st.divider()

# ── Courbes de dégradation ────────────────────────────────────────────────────
st.subheader("Courbes de dégradation")
METRIC_COLORS = [("MAE (MW)", "#2ca02c"), ("RMSE (MW)", "#9467bd"), ("MAPE (%)", "#d62728")]
cols = st.columns(3)

for col, (metric, color) in zip(cols, METRIC_COLORS):
    with col:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=hm_df.index, y=hm_df[metric],
                mode="lines+markers", name=metric,
                line=dict(color=color, width=2),
                marker=dict(size=7),
            )
        )
        fig.add_hline(
            y=hm_df[metric].iloc[0], line_dash="dash", line_color=color,
            opacity=0.5, annotation_text=f"t+1 : {hm_df[metric].iloc[0]:.1f}",
        )
        fig.update_layout(
            title=f"Dégradation — {metric}",
            xaxis_title="Horizon (jours)", yaxis_title=metric,
            xaxis=dict(tickvals=list(hm_df.index)[::2], tickangle=0),
        )
        st.plotly_chart(fig, width="stretch")

st.divider()

# ── Boxplot par horizon ────────────────────────────────────────────────────────
st.subheader("Distribution des erreurs par horizon")
fig = px.box(
    recursive_df, x="horizon", y="abs_error",
    title="Distribution des |erreurs| par horizon (prévisions récursives)",
    labels={"horizon": "Horizon (jours)", "abs_error": "|Erreur| (MW)"},
    color_discrete_sequence=["#9ecae1"],
)
st.plotly_chart(fig, width="stretch")

st.divider()

# ── Exemples de trajectoires ──────────────────────────────────────────────────
st.subheader("Exemples de trajectoires")
sample_starts = sorted(recursive_df["start_date"].unique())
n_ex = min(3, len(sample_starts))
indices = [0, len(sample_starts) // 2, len(sample_starts) - 1][:n_ex]
sample_starts = [sample_starts[i] for i in indices]

cols = st.columns(n_ex)
for col, start in zip(cols, sample_starts):
    sub = recursive_df[recursive_df["start_date"] == start].sort_values("horizon")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sub["horizon"], y=sub["actual"], mode="lines+markers",
        name="Réel", line=dict(color="#1f77b4", width=2), marker=dict(size=6),
    ))
    fig.add_trace(go.Scatter(
        x=sub["horizon"], y=sub["predicted"], mode="lines+markers",
        name="Prédit (récursif)",
        line=dict(color="#d62728", width=2, dash="dash"),
        marker=dict(size=6, symbol="square"),
    ))
    fig.update_layout(
        title=f"Départ : {pd.Timestamp(start).date()}",
        xaxis_title="Horizon (jours)", yaxis_title="Consommation (MW)",
        xaxis=dict(tickvals=list(sub["horizon"])[::2], tickangle=0),
    )
    col.plotly_chart(fig, width="stretch")
