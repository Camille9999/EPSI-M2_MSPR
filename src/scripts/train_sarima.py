"""Train SARIMA/SARIMAX models for electricity consumption forecasting

Each training run creates a **dedicated subdirectory** inside ``src/models/``
(or ``--output-dir``) named after the ``run_id`` timestamp:

  {output_dir}/{run_id}/
    sarima_{run_id}.pkl              — fitted SARIMAXResults (joblib)
    scaler_{run_id}.pkl              — StandardScaler for continuous exog columns
    sarima_run_{run_id}.json          — complete run metadata (all hyperparams + metrics)
    sarima_run_{run_id}_report.md     — human-readable monitoring report
  {output_dir}/
    sarima_metadata.json              — run registry (latest pointer + cumulative history)

Feature selection
-----------------
Default features are selected from the SARIMAX probe model analysis (p-value < 0.001):
  - temp_pc_01, temp_pc_02, temp_pc_03  — PCA temperature components
  - production_mw_lag1                  — lagged electricity production

Calendar cyclicals (dow_sin/cos, doy_sin/cos, is_weekend) were excluded:
their p-values were ≈ 1.000 or > 0.10 in the full-sample analysis.
Override the feature list at training time with ``--features``.

Retraining on new data
-----------------------
Use ``--train-start`` / ``--train-end`` (YYYY-MM-DD) to restrict the training
window for incremental retraining without re-running bronze→silver:

    python src/scripts/train_sarima.py --train-start 2020-01-01
    python src/scripts/train_sarima.py --train-start 2018-01-01 --train-end 2023-12-31

Usage
-----
    # Default run (selected features, full date range):
    python src/scripts/train_sarima.py

    # Explicit orders (from SARIMAX probe results):
    python src/scripts/train_sarima.py --p 2 --d 0 --q 1 --P 2 --D 1 --Q 0 --m 7

    # Custom feature set:
    python src/scripts/train_sarima.py --features temp_pc_01,temp_pc_02,production_mw_lag1

    # Retrain on recent data only:
    python src/scripts/train_sarima.py --train-start 2022-01-01

    # Custom output directory:
    python src/scripts/train_sarima.py --output-dir /models
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
)
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX

# Allow ``from utils.load_env import ...`` regardless of the working directory.
sys.path.insert(0, str(Path(__file__).parent))
from utils.load_env import PATH_DATA  # noqa: E402


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants (mirroring notebook 03_sarima_consumption_forecast)
# ---------------------------------------------------------------------------

FORBIDDEN_COLUMNS: frozenset[str] = frozenset({"prevision_j1_mw", "prevision_j_mw"})

# Features selected from the SARIMAX probe model analysis (p-value < 0.001).
# Calendar cyclicals (dow_sin/cos, doy_sin/cos, is_weekend) are excluded:
# their p-values were ≈ 1.000 or > 0.10 in the full-sample probe model.
DEFAULT_FEATURES: list[str] = [
    "temp_pc_01",
    "temp_pc_02",
    "temp_pc_03",
    "production_mw_lag1",
]

# Columns that must be standardised before fitting.
_SCALE_ELIGIBLE: frozenset[str] = frozenset(
    {c for c in DEFAULT_FEATURES if c.startswith("temp_pc_")} | {"production_mw_lag1"}
)

# Default SARIMA orders — from SARIMAX(2, 0, 1)x(2, 1, [], 7) probe results.
# Override via CLI after running auto_arima in notebook 03.
DEFAULT_P, DEFAULT_D, DEFAULT_Q = 2, 0, 1
DEFAULT_P_S, DEFAULT_D_S, DEFAULT_Q_S, DEFAULT_M = 2, 1, 0, 7

# Maximum number of runs kept in the registry (sarima_metadata.json).
REGISTRY_MAX_RUNS: int = 20


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_features(raw: str) -> list[str]:
    """Parse a comma-separated feature list, stripping whitespace."""
    return [f.strip() for f in raw.split(",") if f.strip()]


def _build_parser() -> argparse.ArgumentParser:
    if PATH_DATA:
        default_silver = Path(PATH_DATA) / "silver" / "rte_sqr_daily_silver.parquet"
    else:
        default_silver = (
            Path(__file__).resolve().parents[1] / "data" / "silver" / "rte_sqr_daily_silver.parquet"
        )

    default_output = Path(__file__).resolve().parents[1] / "models"

    parser = argparse.ArgumentParser(
        description="Train final SARIMA/SARIMAX models for electricity consumption forecasting.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Data paths
    grp_paths = parser.add_argument_group("Paths")
    grp_paths.add_argument(
        "--silver-path",
        type=Path,
        default=default_silver,
        help="Path to the silver parquet dataset (output of bronze_to_silver.py).",
    )
    grp_paths.add_argument(
        "--output-dir",
        type=Path,
        default=default_output,
        help="Directory where model artefacts will be written.",
    )
    grp_paths.add_argument(
        "--run-id",
        type=str,
        default=None,
        help=(
            "Unique identifier for this run. "
            "Auto-generated from UTC timestamp (YYYYMMDDTHHMMSSz) if not provided."
        ),
    )

    # Training date range
    grp_dates = parser.add_argument_group("Training date range")
    grp_dates.add_argument(
        "--train-start",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Earliest training date (inclusive). Defaults to start of the silver dataset.",
    )
    grp_dates.add_argument(
        "--train-end",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Latest training date (inclusive). Defaults to end of the silver dataset.",
    )

    # Feature selection
    grp_features = parser.add_argument_group("Feature selection")
    grp_features.add_argument(
        "--features",
        type=_parse_features,
        default=DEFAULT_FEATURES,
        metavar="f1,f2,...",
        help=(
            "Comma-separated list of exogenous features. "
            "Available: temp_pc_* (PCA components), production_mw_lag1, "
            "dow_sin, dow_cos, doy_sin, doy_cos, is_weekend. "
            f"Default: {','.join(DEFAULT_FEATURES)}"
        ),
    )

    # SARIMA non-seasonal orders
    grp_orders = parser.add_argument_group("SARIMA non-seasonal orders (p, d, q)")
    grp_orders.add_argument("--p", type=int, default=DEFAULT_P, help="AR order.")
    grp_orders.add_argument("--d", type=int, default=DEFAULT_D, help="Integration order.")
    grp_orders.add_argument("--q", type=int, default=DEFAULT_Q, help="MA order.")

    # SARIMA seasonal orders
    grp_seasonal = parser.add_argument_group("SARIMA seasonal orders (P, D, Q, m)")
    grp_seasonal.add_argument("--P", dest="P_s", type=int, default=DEFAULT_P_S, help="Seasonal AR order.")
    grp_seasonal.add_argument("--D", dest="D_s", type=int, default=DEFAULT_D_S, help="Seasonal integration order.")
    grp_seasonal.add_argument("--Q", dest="Q_s", type=int, default=DEFAULT_Q_S, help="Seasonal MA order.")
    grp_seasonal.add_argument("--m", type=int, default=DEFAULT_M, help="Seasonal period (7 = weekly).")

    return parser


# ---------------------------------------------------------------------------
# Data preparation  (mirrors notebook cells: series loading + exog construction)
# ---------------------------------------------------------------------------


def load_and_prepare(
    silver_path: Path,
    train_start: str | None = None,
    train_end: str | None = None,
) -> tuple[pd.Series, pd.DataFrame, list[str]]:
    """Load the silver parquet and return ``(ts, exog_all, scale_columns)``.

    Parameters
    ----------
    silver_path:
        Path to the silver parquet file produced by ``bronze_to_silver.py``.
    train_start, train_end:
        Optional ISO date strings (``YYYY-MM-DD``) to restrict the training window.
        Both bounds are inclusive.

    Returns
    -------
    ts : pd.Series
        Daily electricity consumption series with a ``DatetimeIndex`` at frequency ``"D"``.
    exog_all : pd.DataFrame
        Full matrix of all buildable exogenous features (same index as ``ts``).
        Pass to ``select_features()`` to extract the desired subset.
    scale_columns : list[str]
        Names of the scale-eligible columns present in ``exog_all``.
    """
    if not silver_path.exists():
        raise FileNotFoundError(
            f"Silver parquet not found: {silver_path}\n"
            "Run src/scripts/bronze_to_silver.py first."
        )

    logger.info("Loading silver dataset from %s", silver_path)
    raw_df = pd.read_parquet(silver_path)
    raw_df["date"] = pd.to_datetime(raw_df["date"], errors="coerce")
    raw_df = raw_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # ── Leakage guard ────────────────────────────────────────────────────────
    leaked = FORBIDDEN_COLUMNS.intersection(raw_df.columns)
    if leaked:
        logger.warning("Forbidden columns detected in dataset (will be ignored): %s", leaked)

    pca_columns: list[str] = [c for c in raw_df.columns if c.startswith("temp_pc_")]
    logger.info("Dataset shape: %s | PCA temperature columns: %s", raw_df.shape, pca_columns)

    # ── Target series ────────────────────────────────────────────────────────
    ts = raw_df.set_index("date")["consommation_mw"].asfreq("D")
    n_missing = int(ts.isna().sum())
    if n_missing > 0:
        logger.warning("%d missing day(s) after asfreq — interpolating (method='time')", n_missing)
        ts = ts.interpolate(method="time")

    # ── Build all available exogenous features ───────────────────────────────
    # Mirrors the "Construction des variables exogènes" cell in notebook 03.
    # All buildable features are returned; use select_features() to subset.
    exog_all = raw_df.set_index("date")[["production_mw"] + pca_columns].asfreq("D")
    exog_all = exog_all.interpolate(method="time")

    # Calendar cyclicals (available via --features override; not in default set)
    exog_all["dow_sin"] = np.sin(2 * np.pi * exog_all.index.dayofweek / 7)
    exog_all["dow_cos"] = np.cos(2 * np.pi * exog_all.index.dayofweek / 7)
    exog_all["doy_sin"] = np.sin(2 * np.pi * exog_all.index.dayofyear / 365.25)
    exog_all["doy_cos"] = np.cos(2 * np.pi * exog_all.index.dayofyear / 365.25)
    exog_all["is_weekend"] = (exog_all.index.dayofweek >= 5).astype(float)

    # Lag-1 production to avoid future-data leakage
    exog_all["production_mw_lag1"] = exog_all["production_mw"].shift(1)
    exog_all = exog_all.drop(columns=["production_mw"])
    exog_all = exog_all.dropna().astype("float64")

    # ── Align target and exog on common dates ────────────────────────────────
    common_index = ts.index.intersection(exog_all.index)
    ts = ts.loc[common_index]
    exog_all = exog_all.loc[common_index]

    # Final leakage guard (defensive)
    leaked_exog = FORBIDDEN_COLUMNS.intersection(exog_all.columns)
    if leaked_exog:
        raise RuntimeError(f"Data leakage detected in exog columns: {leaked_exog}")

    # ── Training date range filter ───────────────────────────────────────────
    if train_start is not None:
        start = pd.Timestamp(train_start)
        ts = ts.loc[ts.index >= start]
        exog_all = exog_all.loc[exog_all.index >= start]
        logger.info("Training window start: %s", start.date())

    if train_end is not None:
        end = pd.Timestamp(train_end)
        ts = ts.loc[ts.index <= end]
        exog_all = exog_all.loc[exog_all.index <= end]
        logger.info("Training window end: %s", end.date())

    if ts.empty:
        raise ValueError(
            f"No training data in the requested range [{train_start}, {train_end}]. "
            "Check --train-start / --train-end values."
        )

    scale_columns: list[str] = [c for c in exog_all.columns if c in _SCALE_ELIGIBLE]

    logger.info(
        "Training period: %s → %s (%d days)",
        ts.index.min().date(),
        ts.index.max().date(),
        len(ts),
    )
    logger.info("All available exog columns: %s", list(exog_all.columns))
    logger.info("Scale-eligible columns: %s", scale_columns)

    return ts, exog_all, scale_columns


def select_features(exog_all: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    """Return a subset of ``exog_all`` matching the requested feature list.

    Raises
    ------
    ValueError
        If any requested feature is not present in ``exog_all``.
    """
    missing = [f for f in features if f not in exog_all.columns]
    if missing:
        raise ValueError(
            f"Requested feature(s) not available: {missing}\n"
            f"Available columns: {list(exog_all.columns)}"
        )
    return exog_all[features].copy()


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------


def fit_model(
    ts: pd.Series,
    exog_df: pd.DataFrame,
    order: tuple[int, int, int],
    seasonal_order: tuple[int, int, int, int],
    scale_cols: list[str],
) -> tuple:
    """Fit a final SARIMAX model on the full series.

    Parameters
    ----------
    ts : pd.Series
        Full daily consumption series.
    exog_df : pd.DataFrame
        Exogenous features (aligned with ``ts``).
    order, seasonal_order :
        SARIMA orders.
    scale_cols :
        Continuous columns to standardise before fitting.

    Returns
    -------
    (model_result, scaler, exog_columns) :
        The fitted ``SARIMAXResults``, the fitted ``StandardScaler``, and the
        ordered list of exogenous column names.
    """
    logger.info(
        "Fitting SARIMAX%s × %s  (n=%d days) …",
        order,
        seasonal_order,
        len(ts),
    )

    y = ts.astype("float64")
    x_df = exog_df.copy().astype("float64")

    cols_to_scale = [c for c in scale_cols if c in x_df.columns]
    scaler = StandardScaler()
    if cols_to_scale:
        x_df[cols_to_scale] = scaler.fit_transform(x_df[cols_to_scale]).astype("float64")
    else:
        # Dummy fit so the scaler artefact is always serialisable and consistent.
        scaler.fit(np.zeros((len(x_df), 1)))

    x_np = x_df.to_numpy(dtype="float64")
    model_result = SARIMAX(
        y,
        exog=x_np,
        order=order,
        seasonal_order=seasonal_order,
        enforce_stationarity=False,
        enforce_invertibility=False,
    ).fit(disp=False)

    logger.info(
        "Fit complete — AIC=%.2f  BIC=%.2f  Log-likelihood=%.2f",
        model_result.aic,
        model_result.bic,
        model_result.llf,
    )
    return model_result, scaler, list(x_df.columns)


# ---------------------------------------------------------------------------
# In-sample metrics  (Bloc 3 — monitoring baseline)
# ---------------------------------------------------------------------------


def compute_insample_metrics(model_result, ts: pd.Series) -> dict[str, float]:
    """Compute in-sample regression metrics from model residuals.

    These metrics serve as a **monitoring baseline**: once the model is in
    production, degradation relative to these values signals a need for
    re-training.

    Returns an empty dict if fitted values are not available.
    """
    fitted = model_result.fittedvalues
    if fitted is None or fitted.isna().all():
        logger.warning("Fitted values not available; in-sample metrics skipped.")
        return {}

    y_true = ts.astype("float64").reindex(fitted.index).dropna()
    y_fit = fitted.reindex(y_true.index).dropna()
    common = y_true.index.intersection(y_fit.index)
    y_true = y_true.loc[common].to_numpy()
    y_fit = y_fit.loc[common].to_numpy()

    if len(y_true) == 0:
        return {}

    mae = float(mean_absolute_error(y_true, y_fit))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_fit)))
    mape = float(mean_absolute_percentage_error(y_true, y_fit) * 100)

    return {
        "insample_MAE_MW": round(mae, 2),
        "insample_RMSE_MW": round(rmse, 2),
        "insample_MAPE_pct": round(mape, 4),
    }


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def save_artefacts(
    output_dir: Path,
    run_id: str,
    model_result,
    scaler: StandardScaler,
    exog_columns: list[str],
    order: tuple,
    seasonal_order: tuple,
    ts: pd.Series,
    train_start: str | None,
    train_end: str | None,
    metrics: dict[str, float],
) -> dict:
    """Serialise model and scaler with a unique run-stamped filename."""
    output_dir.mkdir(parents=True, exist_ok=True)

    model_path = output_dir / f"sarima_{run_id}.pkl"
    scaler_path = output_dir / f"scaler_{run_id}.pkl"

    joblib.dump(model_result, model_path)
    joblib.dump(scaler, scaler_path)

    logger.info("Saved model  → %s", model_path)
    logger.info("Saved scaler → %s", scaler_path)

    return {
        "model_type": "SARIMAX",
        "order": list(order),
        "seasonal_order": list(seasonal_order),
        "features": exog_columns,
        "scale_columns": [c for c in exog_columns if c in _SCALE_ELIGIBLE],
        "target_column": "consommation_mw",
        "training_start": str(ts.index.min().date()),
        "training_end": str(ts.index.max().date()),
        "n_training_days": int(len(ts)),
        "train_start_filter": train_start,
        "train_end_filter": train_end,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "artefacts": {
            "model": model_path.name,
            "scaler": scaler_path.name,
            # pca_pipeline and pca_columns are populated later by copy_pca_artefacts()
        },
        "goodness_of_fit": {
            "aic": round(float(model_result.aic), 2),
            "bic": round(float(model_result.bic), 2),
            "log_likelihood": round(float(model_result.llf), 2),
        },
        "insample_metrics": metrics,
    }


# ---------------------------------------------------------------------------
# PCA artefact propagation
# ---------------------------------------------------------------------------


def copy_pca_artefacts(silver_path: Path, run_dir: Path, meta: dict) -> None:
    """Copy ``pca_pipeline.pkl`` and ``pca_columns.json`` from the silver directory
    into *run_dir* and register them in *meta['artefacts']*.

    If the files are not found (old silver build without persistence) a warning
    is emitted and inference will fall back to raw PCA inputs.
    """
    silver_dir = silver_path.parent
    src_pipeline = silver_dir / "pca_pipeline.pkl"
    src_columns = silver_dir / "pca_columns.json"

    if src_pipeline.exists() and src_columns.exists():
        dst_pipeline = run_dir / "pca_pipeline.pkl"
        dst_columns = run_dir / "pca_columns.json"
        shutil.copy2(src_pipeline, dst_pipeline)
        shutil.copy2(src_columns, dst_columns)
        meta["artefacts"]["pca_pipeline"] = dst_pipeline.name
        meta["artefacts"]["pca_columns"] = dst_columns.name
        logger.info("PCA pipeline copied → %s", dst_pipeline)
    else:
        logger.warning(
            "PCA artefacts not found in silver directory (%s). "
            "Re-run bronze_to_silver.py to generate them.",
            silver_dir,
        )


# ---------------------------------------------------------------------------
# Residuals analysis & training report  (Bloc 3 — monitoring)
# ---------------------------------------------------------------------------


def _compute_residual_stats(model_result) -> dict[str, float]:
    """Descriptive statistics on model residuals (for the monitoring report)."""
    resid = model_result.resid.dropna()
    sigma = float(resid.std())
    return {
        "mean": round(float(resid.mean()), 2),
        "std": round(sigma, 2),
        "min": round(float(resid.min()), 2),
        "max": round(float(resid.max()), 2),
        "skewness": round(float(resid.skew()), 4),
        "kurtosis": round(float(resid.kurtosis()), 4),
        "within_1sigma_pct": round(float((resid.abs() <= sigma).mean() * 100), 1),
        "within_2sigma_pct": round(float((resid.abs() <= 2 * sigma).mean() * 100), 1),
    }


def _write_training_report(
    output_dir: Path,
    run_id: str,
    order: tuple,
    seasonal_order: tuple,
    features: list[str],
    train_start: str | None,
    train_end: str | None,
    meta: dict,
    model_result,
) -> Path:
    """Write a human-readable Markdown monitoring report for this training run."""
    _ALERT_MULTIPLIER = 2.0
    report_path = output_dir / f"sarima_run_{run_id}_report.md"

    trained_at = meta.get("trained_at", "—")
    data_start = meta.get("training_start", "—")
    data_end = meta.get("training_end", "—")
    n_days = meta.get("n_training_days", 0)
    m = meta.get("insample_metrics", {})
    gof = meta.get("goodness_of_fit", {})
    stats = _compute_residual_stats(model_result)

    lines: list[str] = [
        f"# Rapport d'entraînement SARIMA — Run `{run_id}`",
        "",
        "---",
        "",
        "## Configuration",
        "",
        "| Paramètre | Valeur |",
        "|-----------|--------|",
        f"| Run ID | `{run_id}` |",
        f"| Date d'entraînement | {trained_at} |",
        f"| Ordres SARIMA | `{order}` × `{seasonal_order}` |",
        f"| Features | `{', '.join(features)}` |",
        f"| Données — début | {data_start} |",
        f"| Données — fin | {data_end} |",
        f"| Nb jours d'entraînement | {n_days} |",
        f"| Filtre `--train-start` | {train_start or '—'} |",
        f"| Filtre `--train-end` | {train_end or '—'} |",
        "",
        "---",
        "",
        "## Qualité du modèle (métriques in-sample)",
        "",
        "| MAE (MW) | RMSE (MW) | MAPE (%) | AIC | BIC |",
        "|----------|-----------|----------|-----|-----|",
        (
            f"| {m.get('insample_MAE_MW', '—')} "
            f"| {m.get('insample_RMSE_MW', '—')} "
            f"| {m.get('insample_MAPE_pct', '—')} "
            f"| {gof.get('aic', '—')} "
            f"| {gof.get('bic', '—')} |"
        ),
        "",
        "---",
        "",
        "## Analyse des résidus",
        "",
        "| Statistique | Valeur |",
        "|-------------|--------|",
        f"| Moyenne | {stats['mean']:.2f} MW |",
        f"| Écart-type (σ) | {stats['std']:.2f} MW |",
        f"| Min | {stats['min']:.2f} MW |",
        f"| Max | {stats['max']:.2f} MW |",
        f"| Skewness | {stats['skewness']:.4f} |",
        f"| Kurtosis (excess) | {stats['kurtosis']:.4f} |",
        f"| Résidus dans ±1σ | {stats['within_1sigma_pct']:.1f}% |",
        f"| Résidus dans ±2σ | {stats['within_2sigma_pct']:.1f}% |",
        "",
        "---",
        "",
        "## Seuils de monitoring",
        "",
        "> Les métriques in-sample constituent la **baseline de référence**."
        " En production, un dépassement des seuils ci-dessous"
        " indique la nécessité d'un ré-entraînement.",
        "",
        "| Métrique | Baseline | Seuil d'alerte |",
        "|----------|----------|----------------|",
    ]

    for metric_key, label in [
        ("insample_MAE_MW", "MAE (MW)"),
        ("insample_RMSE_MW", "RMSE (MW)"),
        ("insample_MAPE_pct", "MAPE (%)"),
    ]:
        baseline = m.get(metric_key)
        if baseline is not None:
            alert = round(baseline * _ALERT_MULTIPLIER, 2)
            lines.append(f"| {label} | {baseline} | {alert} |")

    art = meta["artefacts"]
    lines += [
        "",
        "---",
        "",
        "## Artefacts générés",
        "",
        f"- `{art['model']}`",
        f"- `{art['scaler']}`",
        f"- `sarima_run_{run_id}.json`",
        f"- `sarima_run_{run_id}_report.md`",
        "",
    ]

    report_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Training report written → %s", report_path)
    return report_path


def _update_registry(
    output_dir: Path,
    run_id: str,
    meta: dict,
) -> None:
    """Append this run to ``sarima_metadata.json`` and keep the last REGISTRY_MAX_RUNS."""
    registry_path = output_dir / "sarima_metadata.json"

    if registry_path.exists():
        with registry_path.open("r", encoding="utf-8") as fh:
            existing = json.load(fh)
        runs: list[dict] = existing.get("runs", [])
    else:
        runs = []

    entry: dict = {
        "run_id": run_id,
        "run_dir": run_id,
        "trained_at": meta.get("trained_at", ""),
        "order": meta.get("order", []),
        "seasonal_order": meta.get("seasonal_order", []),
        "features": meta.get("features", []),
        "training_start": meta.get("training_start", ""),
        "training_end": meta.get("training_end", ""),
        "n_training_days": meta.get("n_training_days", 0),
        "model": {
            "model_file": meta["artefacts"]["model"],
            "scaler_file": meta["artefacts"]["scaler"],
            "metadata_file": f"sarima_run_{run_id}.json",
            "report_file": f"sarima_run_{run_id}_report.md",
            "insample_metrics": meta.get("insample_metrics", {}),
            "goodness_of_fit": meta.get("goodness_of_fit", {}),
        },
    }

    runs.append(entry)
    runs = runs[-REGISTRY_MAX_RUNS:]

    registry = {"latest_run_id": run_id, "n_runs": len(runs), "runs": runs}
    with registry_path.open("w", encoding="utf-8") as fh:
        json.dump(registry, fh, indent=2, ensure_ascii=False)

    logger.info("Registry updated → %s  (%d run(s) tracked)", registry_path, len(runs))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    order: tuple[int, int, int] = (args.p, args.d, args.q)
    seasonal_order: tuple[int, int, int, int] = (args.P_s, args.D_s, args.Q_s, args.m)
    run_id: str = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir: Path = args.output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("SARIMA training — MSPR EPSI Bloc 3")
    logger.info("Run ID   : %s", run_id)
    logger.info("Orders   : SARIMA%s × %s", order, seasonal_order)
    logger.info("Features : %s", args.features)
    logger.info("Window   : [%s, %s]", args.train_start or "start", args.train_end or "end")
    logger.info("Run dir  : %s", run_dir)
    logger.info("=" * 60)

    # 1. Load and prepare data (apply optional date range filter)
    ts, exog_all, scale_columns = load_and_prepare(
        args.silver_path,
        train_start=args.train_start,
        train_end=args.train_end,
    )

    # 2. Select and validate the feature set
    exog_df = select_features(exog_all, args.features)
    logger.info("Selected features: %s", list(exog_df.columns))

    # 3. Train the model and collect metadata
    model_result, scaler, exog_columns = fit_model(
        ts=ts,
        exog_df=exog_df,
        order=order,
        seasonal_order=seasonal_order,
        scale_cols=scale_columns,
    )

    metrics = compute_insample_metrics(model_result, ts)
    logger.info("In-sample metrics: %s", metrics)

    meta = save_artefacts(
        output_dir=run_dir,
        run_id=run_id,
        model_result=model_result,
        scaler=scaler,
        exog_columns=exog_columns,
        order=order,
        seasonal_order=seasonal_order,
        ts=ts,
        train_start=args.train_start,
        train_end=args.train_end,
        metrics=metrics,
    )

    # 3b. Copy PCA artefacts from silver directory into the run directory
    copy_pca_artefacts(args.silver_path, run_dir, meta)

    # 4. Write per-run metadata JSON
    run_meta_path = run_dir / f"sarima_run_{run_id}.json"
    with run_meta_path.open("w", encoding="utf-8") as fh:
        json.dump({"run_id": run_id, "model": meta}, fh, indent=2, ensure_ascii=False)
    logger.info("Run metadata written → %s", run_meta_path)

    # 5. Write training / monitoring report
    _write_training_report(
        output_dir=run_dir,
        run_id=run_id,
        order=order,
        seasonal_order=seasonal_order,
        features=args.features,
        train_start=args.train_start,
        train_end=args.train_end,
        meta=meta,
        model_result=model_result,
    )

    # 6. Update cumulative registry
    _update_registry(
        output_dir=args.output_dir,
        run_id=run_id,
        meta=meta,
    )

    logger.info("=" * 60)
    logger.info("Training complete. Run ID: %s", run_id)
    logger.info("Artefacts in: %s", run_dir)
    logger.info("Registry   : %s", args.output_dir / "sarima_metadata.json")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
