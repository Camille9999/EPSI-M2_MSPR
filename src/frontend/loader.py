"""
Shared data-loading utilities for the SARIMA monitoring dashboard.

All heavy I/O and model loads are cached:
- @st.cache_data   → serialisable objects (DataFrames, dicts, Series)
- @st.cache_resource → non-serialisable objects (SARIMAXResults)
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Paths (override via environment variables for Docker)
# ---------------------------------------------------------------------------

# src/frontend/loader.py  →  parents[1] = src/  →  src/models
MODELS_DIR = Path(
    os.getenv("MODELS_DIR", str(Path(__file__).resolve().parents[1] / "models"))
)
API_URL = os.getenv("API_URL", "http://localhost:8000")
WARMUP_DAYS: int = 60  # matches notebook 07_monitoring_sarima


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@st.cache_data(ttl=60)
def load_registry() -> dict:
    """Load sarima_metadata.json (auto-refreshed every 60 s)."""
    path = MODELS_DIR / "sarima_metadata.json"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


@st.cache_data(ttl=60)
def list_runs() -> list[dict]:
    """All runs from the registry, newest first."""
    return list(reversed(load_registry().get("runs", [])))


@st.cache_data
def load_run_json(run_id: str) -> dict:
    """Load the detailed per-run JSON metadata file."""
    path = MODELS_DIR / run_id / f"sarima_run_{run_id}.json"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _resolve_model_entry(run: dict) -> dict:
    """Return the model metadata dict regardless of registry format (new or legacy)."""
    if "model" in run:
        return run["model"]
    if "models" in run:
        models_dict: dict = run["models"]
        return models_dict.get("h1") or next(iter(models_dict.values()), {})
    return {}


@st.cache_data(ttl=60)
def build_registry_table() -> pd.DataFrame:
    """Flat DataFrame — one row per run with all key metrics."""
    rows = []
    for run in list_runs():
        m_entry = _resolve_model_entry(run)
        im = m_entry.get("insample_metrics", {})
        gof = m_entry.get("goodness_of_fit", {})
        rows.append(
            {
                "run_id": run["run_id"],
                "trained_at": run.get("trained_at", ""),
                "MAE (MW)": im.get("insample_MAE_MW"),
                "RMSE (MW)": im.get("insample_RMSE_MW"),
                "MAPE (%)": im.get("insample_MAPE_pct"),
                "AIC": gof.get("aic"),
                "BIC": gof.get("bic"),
                "n_training_days": run.get("n_training_days"),
                "training_start": run.get("training_start"),
                "training_end": run.get("training_end"),
                "features": ", ".join(run.get("features", [])),
                "order": str(tuple(run.get("order", []))),
                "seasonal_order": str(tuple(run.get("seasonal_order", []))),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["trained_at"] = pd.to_datetime(df["trained_at"], errors="coerce")
        df = df.sort_values("trained_at", ascending=False).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Model loading (kept in memory — expensive objects)
# ---------------------------------------------------------------------------


@st.cache_resource
def load_model_object(run_id: str):
    """Load and cache a SARIMAXResults object."""
    run = next((r for r in list_runs() if r["run_id"] == run_id), None)
    if run is None:
        return None
    m_entry = _resolve_model_entry(run)
    if not m_entry:
        return None
    model_path = MODELS_DIR / run_id / m_entry["model_file"]
    if not model_path.exists():
        return None
    return joblib.load(model_path)


# ---------------------------------------------------------------------------
# Derived DataFrames
# ---------------------------------------------------------------------------


@st.cache_data
def build_insample_df(run_id: str) -> pd.DataFrame:
    """
    Fitted vs actual in-sample DataFrame with error columns.
    WARMUP_DAYS first rows are excluded (model initialisation phase).
    """
    model = load_model_object(run_id)
    if model is None:
        return pd.DataFrame()

    fitted = model.fittedvalues.dropna()
    fitted.index = pd.DatetimeIndex(fitted.index)

    dates = pd.DatetimeIndex(model.model.data.dates)
    endog = model.model.endog.flatten()
    actual = pd.Series(endog, index=dates).reindex(fitted.index).dropna()
    common = actual.index.intersection(fitted.index)

    df = pd.DataFrame(
        {
            "actual": actual.loc[common],
            "fitted": fitted.loc[common],
            "error": fitted.loc[common] - actual.loc[common],
            "abs_error": (fitted.loc[common] - actual.loc[common]).abs(),
        }
    )
    df["month"] = df.index.month
    df["dow"] = df.index.dayofweek
    df["sq_error"] = df["error"] ** 2

    return df.iloc[WARMUP_DAYS:].copy()


@st.cache_data
def build_ts_series(run_id: str) -> pd.Series:
    """Reconstruct the full endog series from the model."""
    model = load_model_object(run_id)
    if model is None:
        return pd.Series(dtype=float)
    dates = pd.DatetimeIndex(model.model.data.dates)
    endog = model.model.endog.flatten()
    return pd.Series(endog, index=dates, name="consommation_mw")


@st.cache_data
def get_residuals(run_id: str) -> pd.Series:
    """Model residuals with WARMUP_DAYS excluded."""
    model = load_model_object(run_id)
    if model is None:
        return pd.Series(dtype=float)
    resid = model.resid.dropna()
    resid.index = pd.DatetimeIndex(resid.index)
    return resid.iloc[WARMUP_DAYS:]
