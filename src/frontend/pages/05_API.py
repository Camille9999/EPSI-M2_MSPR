"""Page 5 — Statut de l'API FastAPI et prédiction en direct."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib import error, request

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from loader import API_URL

st.set_page_config(page_title="Statut API", page_icon="🌐", layout="wide")
st.title("🌐 Statut de l'API de prédiction")

api_url = st.sidebar.text_input("URL de l'API", value=API_URL)
st.sidebar.caption("Modifiable via la variable d'environnement `API_URL`.")


# ── Helpers HTTP ──────────────────────────────────────────────────────────────
def _get(endpoint: str, timeout: float = 5.0) -> tuple[int, dict]:
    try:
        req = request.Request(f"{api_url}{endpoint}", method="GET")
        with request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), json.loads(resp.read().decode())
    except error.HTTPError as e:
        try:
            body = json.loads(e.read().decode())
        except Exception:
            body = {"error": str(e.reason)}
        return int(e.code), body
    except Exception as e:
        return 0, {"error": str(e)}


def _post(endpoint: str, payload: dict, timeout: float = 10.0) -> tuple[int, dict]:
    try:
        body = json.dumps(payload).encode()
        req = request.Request(
            f"{api_url}{endpoint}", method="POST", data=body,
            headers={"Content-Type": "application/json"},
        )
        with request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), json.loads(resp.read().decode())
    except error.HTTPError as e:
        try:
            body = json.loads(e.read().decode())
        except Exception:
            body = {"error": str(e.reason)}
        return int(e.code), body
    except Exception as e:
        return 0, {"error": str(e)}


# ── Health check ──────────────────────────────────────────────────────────────
st.subheader("Health check")
if st.button("🔄 Vérifier /health"):
    status, data = _get("/health")
    st.session_state["health"] = (status, data)

if "health" in st.session_state:
    status, data = st.session_state["health"]
    if status == 200 and data.get("status") == "ok":
        st.success(
            f"✅ API opérationnelle — modèle par défaut : **{data.get('latest_run_id', '—')}** "
            f"({data.get('model_name', '—')})"
        )
    elif status == 200:
        st.warning(f"⚠️ API dégradée — {data}")
    else:
        st.error(f"❌ Inaccessible (HTTP {status}) — {data.get('error', data)}")

st.divider()

# ── Liste des runs disponibles ────────────────────────────────────────────────
st.subheader("Runs disponibles (GET /runs)")
if st.button("📋 Récupérer la liste des runs"):
    status, data = _get("/runs")
    st.session_state["api_runs"] = (status, data)

selected_run_id: str | None = None

if "api_runs" in st.session_state:
    status, data = st.session_state["api_runs"]
    if status == 200 and isinstance(data, list) and data:
        import pandas as pd

        runs_df = pd.DataFrame(data)
        display_cols = [c for c in ["run_id", "trained_at", "training_start", "training_end",
                                     "n_training_days", "mae_mw", "rmse_mw", "mape_pct"]
                        if c in runs_df.columns]
        st.dataframe(runs_df[display_cols], hide_index=True, width="stretch")

        run_options = [r["run_id"] for r in data]
        selected_run_id = st.selectbox(
            "Sélectionner un run pour les métadonnées / la prédiction",
            options=["(latest)"] + run_options,
            index=0,
        )
        if selected_run_id == "(latest)":
            selected_run_id = None
    elif status == 200:
        st.info("Aucun run disponible dans le registre.")
    else:
        st.error(f"HTTP {status} — {data}")

st.divider()

# ── Métadonnées ────────────────────────────────────────────────────────────────
st.subheader("Métadonnées du run sélectionné (GET /metadata)")
meta_endpoint = f"/metadata?run_id={selected_run_id}" if selected_run_id else "/metadata"
if st.button("📋 Récupérer /metadata"):
    status, data = _get(meta_endpoint)
    st.session_state["api_metadata"] = (status, data)

if "api_metadata" in st.session_state:
    status, data = st.session_state["api_metadata"]
    if status == 200:
        c1, c2 = st.columns(2)
        c1.metric("Run ID", data.get("run_id", "—"))
        c2.metric("Modèle", data.get("model_name", "—"))
        c3, c4 = st.columns(2)
        c3.metric("Cible", data.get("target_name", "—"))
        c4.metric("PCA disponible", "✅" if data.get("pca_available") else "❌")
        st.write("**Inputs API :**", data.get("user_inputs", []))
        st.write("**Features internes (SARIMAX) :**", data.get("internal_features", []))
        if data.get("training_start"):
            st.caption(
                f"Fenêtre d'entraînement : {data['training_start']} → "
                f"{data.get('training_end', '—')}  ({data.get('n_training_days', '—')} jours)"
            )
    else:
        st.error(f"HTTP {status} — {data}")

st.divider()

# ── Prédiction en direct ──────────────────────────────────────────────────────
st.subheader("Prédiction en direct (POST /predict)")

# Récupérer dynamiquement les features depuis /metadata du run sélectionné
status_m, meta_data = _get(meta_endpoint, timeout=2.0)
feature_names: list[str] = meta_data.get("internal_features", []) if status_m == 200 else []
pca_available: bool = meta_data.get("pca_available", False) if status_m == 200 else False

if selected_run_id:
    st.caption(f"Run utilisé pour la prédiction : **{selected_run_id}**")
else:
    st.caption("Run utilisé pour la prédiction : **latest** (par défaut)")

if status_m != 200:
    st.warning("⚠️ API inaccessible — vérifie qu'elle est démarrée.")

if not pca_available:
    st.warning(
        "⚠️ Pipeline PCA non disponible pour ce run. "
        "Les composantes de température seront mises à zéro. "
        "Re-lance `bronze_to_silver.py` puis `train_sarima.py` pour résoudre."
    )

st.markdown("**3 inputs attendus :**")
col_a, col_b, col_c = st.columns(3)
temp_min_avg = col_a.number_input(
    "temp_min_avg (°C)", value=10.0, step=0.5,
    help="Moyenne nationale des températures minimales journalières (stations TN_*).",
    key="input_temp_min",
)
temp_max_avg = col_b.number_input(
    "temp_max_avg (°C)", value=20.0, step=0.5,
    help="Moyenne nationale des températures maximales journalières (stations TX_*).",
    key="input_temp_max",
)
production_mw_lag1 = col_c.number_input(
    "production_mw_lag1 (MW)", value=50000.0, step=1000.0,
    help="Production électrique totale du jour précédent (MW).",
    key="input_prod_lag1",
)

if pca_available and feature_names:
    st.caption(
        "ℹ️ Les moyennes température sont transformées en composantes PCA "
        f"(`{', '.join(feature_names)}`) avant d'être passées au modèle SARIMAX."
    )

predict_payload: dict = {
    "temp_min_avg": temp_min_avg,
    "temp_max_avg": temp_max_avg,
    "production_mw_lag1": production_mw_lag1,
}
if selected_run_id:
    predict_payload["run_id"] = selected_run_id

if st.button("⚡ Prédire", type="primary"):
    status_p, pred_data = _post("/predict", predict_payload)
    if status_p == 200:
        pred_val = pred_data.get("prediction", 0)
        st.success(
            f"Prédiction (run **{pred_data.get('run_id', '—')}**) : "
            f"**{pred_val:.1f} MW**"
        )
        pca_vals = pred_data.get("pca_components", [])
        if pca_vals:
            st.caption(
                "Composantes PCA intermédiaires : "
                + ", ".join(f"{v:.4f}" for v in pca_vals)
            )
        st.json(pred_data)
    else:
        st.error(f"HTTP {status_p} — {pred_data}")
