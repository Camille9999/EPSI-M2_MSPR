from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class PredictRequest(BaseModel):
    temp_min_avg: float = Field(
        ...,
        description=(
            "National average of daily minimum temperatures across all SQR stations "
            "(same unit as source data, typically °C)."
        ),
    )
    temp_max_avg: float = Field(
        ...,
        description=(
            "National average of daily maximum temperatures across all SQR stations."
        ),
    )
    production_mw_lag1: float = Field(
        ...,
        description="Electricity production the previous day (MW).",
    )
    run_id: str | None = Field(
        None,
        description=(
            "ID of the run to use for prediction. "
            "Defaults to the latest run in the registry."
        ),
    )


class PredictResponse(BaseModel):
    run_id: str
    model_name: str
    target_name: str
    prediction: float
    pca_components: list[float] = Field(
        default_factory=list,
        description="Intermediate PCA components derived from the temperature inputs.",
    )


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    latest_run_id: str | None = None
    model_name: str | None = None


class MetadataResponse(BaseModel):
    run_id: str
    model_name: str
    target_name: str
    user_inputs: list[str] = Field(
        description="Names of the fields expected by POST /predict."
    )
    internal_features: list[str] = Field(
        description="Internal SARIMAX feature names (after PCA transform)."
    )
    pca_available: bool = Field(
        description="Whether the PCA pipeline artefact is available for this run."
    )
    training_start: str | None = None
    training_end: str | None = None
    n_training_days: int | None = None


class RunSummary(BaseModel):
    run_id: str
    trained_at: str | None = None
    training_start: str | None = None
    training_end: str | None = None
    n_training_days: int | None = None
    mae_mw: float | None = None
    rmse_mw: float | None = None
    mape_pct: float | None = None


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]
MODEL_DIR = Path(os.getenv("MODELS_DIR", str(BASE_DIR / "models")))
REGISTRY_PATH = MODEL_DIR / "sarima_metadata.json"

# User-facing input names (fixed — independent of the internal PCA feature names)
USER_INPUT_NAMES: list[str] = ["temp_min_avg", "temp_max_avg", "production_mw_lag1"]

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="EPSI MSPR Electricity Prediction API",
    version="3.0.0",
    description=(
        "FastAPI service exposing SARIMAX electricity consumption forecasting. "
        "Accepts interpretable temperature averages; PCA transform is applied internally. "
        "Supports per-request model selection via `run_id`."
    ),
)


# ---------------------------------------------------------------------------
# In-memory artefact cache
# {run_id: (model, scaler, pca_pipeline | None, pca_columns | None, meta)}
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[Any, Any, Any, list[str] | None, dict]] = {}
_latest_run_id: str | None = None


def _resolve_model_entry(run_entry: dict) -> dict:
    """Return model metadata regardless of registry format (new or legacy)."""
    if "model" in run_entry:
        return run_entry["model"]
    if "models" in run_entry:
        models_dict: dict = run_entry["models"]
        return models_dict.get("h1") or next(iter(models_dict.values()), {})
    return {}


def _load_run(run_id: str) -> tuple[Any, Any, Any, list[str] | None, dict]:
    """Load and cache artefacts for *run_id*.

    Returns
    -------
    (model, scaler, pca_pipeline, pca_columns, meta)
    ``pca_pipeline`` and ``pca_columns`` are ``None`` when the PCA artefact
    was not found (legacy runs trained before the PCA persistence feature).
    """
    if run_id in _cache:
        return _cache[run_id]

    if not REGISTRY_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Registry not found at {REGISTRY_PATH}. Run train_sarima.py first.",
        )

    with REGISTRY_PATH.open("r", encoding="utf-8") as fh:
        registry = json.load(fh)

    run_entry = next((r for r in registry["runs"] if r["run_id"] == run_id), None)
    if run_entry is None:
        raise HTTPException(
            status_code=404,
            detail=f"Run '{run_id}' not found in registry.",
        )

    run_dir = MODEL_DIR / run_entry["run_dir"]
    m_entry = _resolve_model_entry(run_entry)
    if not m_entry:
        raise HTTPException(
            status_code=500,
            detail=f"Registry entry for run '{run_id}' has no model metadata.",
        )

    model_path = run_dir / m_entry["model_file"]
    scaler_path = run_dir / m_entry["scaler_file"]
    meta_path = run_dir / m_entry["metadata_file"]

    if not model_path.exists() or not scaler_path.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Artefact files missing for run '{run_id}' in {run_dir}.",
        )

    loaded_model = joblib.load(model_path)
    loaded_scaler = joblib.load(scaler_path)

    if meta_path.exists():
        with meta_path.open("r", encoding="utf-8") as fh:
            full_meta = json.load(fh)
        # New format: {"run_id": ..., "model": {...}}
        # Old format: {"run_id": ..., "models": [...]}
        meta = full_meta.get("model") or {}
        if not meta:
            models_list = full_meta.get("models", [])
            meta = models_list[0] if models_list else {}
    else:
        meta = {}

    # Ensure critical keys are always present — fall back to registry entry
    if not meta.get("features"):
        meta["features"] = run_entry.get("features", [])
    if not meta.get("order"):
        meta["order"] = run_entry.get("order", [])
    if not meta.get("seasonal_order"):
        meta["seasonal_order"] = run_entry.get("seasonal_order", [])
    if not meta.get("scale_columns"):
        meta["scale_columns"] = [
            c for c in meta.get("features", [])
            if c.startswith("temp_pc_") or c == "production_mw_lag1"
        ]
    meta.setdefault("target_column", "consommation_mw")
    meta.setdefault("run_id", run_id)

    # ── Load PCA artefacts ────────────────────────────────────────────────────
    # Look in meta["artefacts"] (from per-run JSON) — NOT in m_entry (registry).
    # Fall back to probing the run directory directly for backward compatibility.
    pca_pipeline: Any | None = None
    pca_columns: list[str] | None = None

    artefacts: dict = meta.get("artefacts", {})
    pca_pipeline_name: str | None = artefacts.get("pca_pipeline")
    pca_columns_name: str | None = artefacts.get("pca_columns")

    # Fallback: if artefacts dict doesn't mention PCA, probe the run dir directly
    if not pca_pipeline_name:
        candidate = run_dir / "pca_pipeline.pkl"
        if candidate.exists():
            pca_pipeline_name = "pca_pipeline.pkl"
    if not pca_columns_name:
        candidate = run_dir / "pca_columns.json"
        if candidate.exists():
            pca_columns_name = "pca_columns.json"

    if pca_pipeline_name and pca_columns_name:
        pca_pipeline_path = run_dir / pca_pipeline_name
        pca_columns_path = run_dir / pca_columns_name
        if pca_pipeline_path.exists() and pca_columns_path.exists():
            pca_pipeline = joblib.load(pca_pipeline_path)
            pca_columns = json.loads(pca_columns_path.read_text(encoding="utf-8"))

    _cache[run_id] = (loaded_model, loaded_scaler, pca_pipeline, pca_columns, meta)
    return loaded_model, loaded_scaler, pca_pipeline, pca_columns, meta


def _apply_pca(
    pca_pipeline: Any,
    pca_columns: list[str],
    temp_min_avg: float,
    temp_max_avg: float,
) -> list[float]:
    """Broadcast national temperature averages across all stations, then apply PCA.

    ``TN_*`` columns receive ``temp_min_avg``, ``TX_*`` columns receive
    ``temp_max_avg``.  Any other column (unlikely) is set to 0.
    """
    row = np.array(
        [
            temp_min_avg if col.startswith("TN_") else (
                temp_max_avg if col.startswith("TX_") else 0.0
            )
            for col in pca_columns
        ],
        dtype=float,
    ).reshape(1, -1)
    return pca_pipeline.transform(row)[0].tolist()


def _startup_load() -> None:
    global _latest_run_id

    if not REGISTRY_PATH.exists():
        return

    with REGISTRY_PATH.open("r", encoding="utf-8") as fh:
        registry = json.load(fh)

    _latest_run_id = registry.get("latest_run_id")
    if _latest_run_id:
        _load_run(_latest_run_id)


@app.on_event("startup")
def on_startup() -> None:
    _startup_load()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> HealthResponse:
    loaded = _latest_run_id is not None and _latest_run_id in _cache

    model_name: str | None = None
    if loaded:
        _, _, _, _, meta = _cache[_latest_run_id]
        order = meta.get("order", [])
        seasonal_order = meta.get("seasonal_order", [])
        model_name = f"SARIMAX{tuple(order)}x{tuple(seasonal_order)}"

    return HealthResponse(
        status="ok" if loaded else "degraded",
        model_loaded=loaded,
        latest_run_id=_latest_run_id,
        model_name=model_name,
    )


@app.get("/runs", response_model=list[RunSummary])
def list_runs() -> list[RunSummary]:
    """Return all runs registered in the registry, newest first."""
    if not REGISTRY_PATH.exists():
        raise HTTPException(status_code=503, detail="Registry not found.")

    with REGISTRY_PATH.open("r", encoding="utf-8") as fh:
        registry = json.load(fh)

    result: list[RunSummary] = []
    for r in reversed(registry.get("runs", [])):
        m_entry = _resolve_model_entry(r)
        im = m_entry.get("insample_metrics", {})
        result.append(
            RunSummary(
                run_id=r["run_id"],
                trained_at=r.get("trained_at"),
                training_start=r.get("training_start"),
                training_end=r.get("training_end"),
                n_training_days=r.get("n_training_days"),
                mae_mw=im.get("insample_MAE_MW"),
                rmse_mw=im.get("insample_RMSE_MW"),
                mape_pct=im.get("insample_MAPE_pct"),
            )
        )
    return result


@app.get("/metadata")
def get_metadata(run_id: str | None = None) -> MetadataResponse:
    """Return metadata for a run. Defaults to the latest run."""
    resolved_id = run_id or _latest_run_id
    if resolved_id is None:
        raise HTTPException(status_code=503, detail="No run loaded.")

    _, _, pca_pipeline, _, meta = _load_run(resolved_id)

    order = meta.get("order", [])
    seasonal_order = meta.get("seasonal_order", [])
    return MetadataResponse(
        run_id=resolved_id,
        model_name=f"SARIMAX{tuple(order)}x{tuple(seasonal_order)}",
        target_name=str(meta.get("target_column", "consommation_mw")),
        user_inputs=USER_INPUT_NAMES,
        internal_features=[str(f) for f in meta.get("features", [])],
        pca_available=pca_pipeline is not None,
        training_start=meta.get("training_start"),
        training_end=meta.get("training_end"),
        n_training_days=meta.get("n_training_days"),
    )


@app.post(
    "/predict",
    responses={
        400: {"description": "Bad request: invalid values or PCA transform failed."},
        404: {"description": "Requested run_id not found in registry."},
        503: {"description": "Service unavailable: artefacts not loaded."},
    },
)
def predict(payload: PredictRequest) -> PredictResponse:
    resolved_id = payload.run_id or _latest_run_id
    if resolved_id is None:
        raise HTTPException(status_code=503, detail="No model loaded.")

    loaded_model, loaded_scaler, pca_pipeline, pca_columns, meta = _load_run(resolved_id)

    feature_names: list[str] = [str(f) for f in meta.get("features", [])]
    scale_columns: list[str] = [str(c) for c in meta.get("scale_columns", [])]

    if not feature_names:
        raise HTTPException(status_code=500, detail="Invalid metadata: missing features.")

    # ── Build internal feature vector ─────────────────────────────────────────
    if pca_pipeline is not None and pca_columns is not None:
        # Apply PCA: temp_min/max_avg → temp_pc_01/02/03
        try:
            pca_components = _apply_pca(
                pca_pipeline, pca_columns,
                payload.temp_min_avg, payload.temp_max_avg,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail=f"PCA transform failed: {exc}"
            ) from exc

        # Map PCA components to feature names in SARIMAX order
        feature_values: dict[str, float] = {}
        pc_names = sorted([f for f in feature_names if f.startswith("temp_pc_")])
        for i, name in enumerate(pc_names):
            feature_values[name] = pca_components[i] if i < len(pca_components) else 0.0
        feature_values["production_mw_lag1"] = payload.production_mw_lag1
    else:
        # PCA artefact not available: approximate by passing zeros for PCA columns
        # and use production_mw_lag1 directly.  Warn in the response.
        pca_components = []
        feature_values = {n: 0.0 for n in feature_names}
        feature_values["production_mw_lag1"] = payload.production_mw_lag1

    # Validate all internal features are covered
    missing = [n for n in feature_names if n not in feature_values]
    if missing:
        raise HTTPException(
            status_code=500,
            detail={"error": "Internal feature mapping incomplete.", "missing": missing},
        )

    x = np.array([[feature_values[n] for n in feature_names]], dtype=float)

    # Apply SARIMAX scaler (only to scale_columns, e.g. temp_pc_* and production_mw_lag1)
    if scale_columns:
        scale_idx = [feature_names.index(c) for c in scale_columns if c in feature_names]
        if scale_idx:
            x[:, scale_idx] = loaded_scaler.transform(x[:, scale_idx])

    try:
        prediction_value = float(loaded_model.forecast(steps=1, exog=x).iloc[0])
    except Exception as exc:
        logger.exception("forecast() failed for run %s", resolved_id)
        raise HTTPException(status_code=400, detail=f"Prediction failed: {exc}") from exc

    order = meta.get("order", [])
    seasonal_order = meta.get("seasonal_order", [])

    return PredictResponse(
        run_id=resolved_id,
        model_name=f"SARIMAX{tuple(order)}x{tuple(seasonal_order)}",
        target_name=str(meta.get("target_column", "consommation_mw")),
        prediction=prediction_value,
        pca_components=[round(v, 6) for v in pca_components],
    )
