"""Home page — SARIMA Monitoring Dashboard."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from loader import MODELS_DIR, build_registry_table, list_runs, load_registry

APP_NAME = "MSPR Energy App"
APP_VERSION = "v1.0.0"

st.set_page_config(
    page_title="SARIMA Monitoring — EDF",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.caption(f"{APP_NAME}  \nversion : {APP_VERSION}")
st.title("⚡ SARIMA Monitoring — Consommation électrique EDF")
st.markdown(
    "Dashboard de suivi des modèles SARIMAX entraînés sur les données RTE éco2mix et les Séries Quotidiennes de Référence (SQR) de Météo France."
)
st.divider()

# ── Chargement du registre ────────────────────────────────────────────────────
registry = load_registry()
runs = list_runs()

if not registry:
    st.error(
        f"Registre introuvable : `{MODELS_DIR / 'sarima_metadata.json'}`  \n"
        "Lance d'abord `python src/scripts/train_sarima.py`."
    )
    st.stop()

# ── Métriques globales ────────────────────────────────────────────────────────
c1, c2 = st.columns(2)
c1.metric("Runs enregistrés", registry.get("n_runs", len(runs)))
c2.metric("Dernier run", registry.get("latest_run_id", "—"))
c3, c4 = st.columns(2)
if runs:
    latest = runs[0]
    c3.metric("Jours d'entraînement", latest.get("n_training_days", "—"))
    c4.metric(
        "Période d'entraînement",
        f"{latest.get('training_start', '?')} → {latest.get('training_end', '?')}",
    )

st.divider()

# ── Tableau de tous les runs ──────────────────────────────────────────────────
st.subheader("Runs disponibles")
df = build_registry_table()

if df.empty:
    st.info("Aucun run disponible.")
else:
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
                "n_training_days",
                "features",
            ]
        ],
        width="stretch",
        hide_index=True,
    )

st.caption(f"Dossier des modèles : `{MODELS_DIR}`")
