"""Simulate historical SARIMAX training — one run every 6 months.

Creates one training run per 6-month window, starting from 2013-12-31 and
advancing to the last available date in the silver dataset.  Training always
starts from the beginning of the available data (no ``--train-start`` filter).

Each run's ``run_id`` and ``trained_at`` are fixed to the training-window end
date (midnight UTC) so that the Streamlit temporal-evolution chart correctly
reflects the progression over time.

Run IDs look like real timestamps but carry the simulated date:
    20131231T000000Z  →  training window 2012-01-01 … 2013-12-31
    20140630T000000Z  →  training window 2012-01-01 … 2014-06-30
    …

Usage
-----
    # Show the plan without training:
    python src/scripts/simulate_historical_training.py --dry-run

    # Run everything (overwrites the existing registry):
    python src/scripts/simulate_historical_training.py

    # Resume a partial run (re-uses existing artefacts, re-trains missing ones):
    python src/scripts/simulate_historical_training.py --skip-existing

    # Custom paths:
    python src/scripts/simulate_historical_training.py \\
        --silver-path src/data/silver/rte_sqr_daily_silver.parquet \\
        --output-dir  src/models
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

# Allow ``from utils.load_env import ...`` regardless of the working directory.
sys.path.insert(0, str(Path(__file__).parent))

# Import the training module as a whole (so monkey-patching REGISTRY_MAX_RUNS works)
# and also import individual helpers for direct use.
import train_sarima as _ts
from train_sarima import (
    DEFAULT_FEATURES,
    _update_registry,
    _write_training_report,
    compute_insample_metrics,
    copy_pca_artefacts,
    fit_model,
    load_and_prepare,
    save_artefacts,
    select_features,
)
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

# Increase the registry cap for the simulation (default is 20, we may have ~25 runs).
_ts.REGISTRY_MAX_RUNS = 50

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_silver_path() -> Path:
    if PATH_DATA:
        return Path(PATH_DATA) / "silver" / "rte_sqr_daily_silver.parquet"
    return (
        Path(__file__).resolve().parents[1] / "data" / "silver" / "rte_sqr_daily_silver.parquet"
    )


def _default_output_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "models"


def _generate_checkpoints(max_date: pd.Timestamp) -> list[pd.Timestamp]:
    """Return end-of-half-year dates from 2013-H2 up to *max_date* (inclusive).

    Alternates between:
        H2 → December 31  (2013-12-31, 2014-12-31, …)
        H1 → June 30      (2014-06-30, 2015-06-30, …)
    """
    checkpoints: list[pd.Timestamp] = []
    year = 2013
    half = 2  # start at H2 (December 31)

    while True:
        d = pd.Timestamp(f"{year}-06-30") if half == 1 else pd.Timestamp(f"{year}-12-31")
        if d > max_date:
            break
        checkpoints.append(d)

        # Advance: H2 → next-year H1,  H1 → same-year H2
        if half == 2:
            half = 1
            year += 1
        else:
            half = 2

    return checkpoints


def _run_id(d: pd.Timestamp) -> str:
    """Simulated run_id — timestamp format at midnight UTC for date *d*."""
    return d.strftime("%Y%m%dT000000Z")


def _trained_at(d: pd.Timestamp) -> str:
    """ISO 8601 string for simulated training date (midnight UTC)."""
    return d.strftime("%Y-%m-%dT00:00:00+00:00")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Simulate historical SARIMAX training — one run every 6 months.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--silver-path",
        type=Path,
        default=_default_silver_path(),
        help="Path to the silver parquet dataset.",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=_default_output_dir(),
        help="Directory where model artefacts will be written.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the training plan without actually training anything.",
    )
    p.add_argument(
        "--skip-existing",
        action="store_true",
        help=(
            "Skip runs whose output directory already exists and re-register "
            "them from their existing JSON.  Useful to resume an interrupted run."
        ),
    )
    return p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _build_parser().parse_args()

    # ── Detect max available date ────────────────────────────────────────────
    logger.info("Loading silver parquet to detect max available date …")
    if not args.silver_path.exists():
        raise FileNotFoundError(
            f"Silver parquet not found: {args.silver_path}\n"
            "Run src/scripts/bronze_to_silver.py first."
        )
    raw = pd.read_parquet(args.silver_path, columns=["date"])
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    max_date = raw["date"].max()
    logger.info("Max available date: %s", max_date.date())

    # ── Generate checkpoints ─────────────────────────────────────────────────
    checkpoints = _generate_checkpoints(max_date)
    logger.info("Training plan: %d runs", len(checkpoints))
    for i, d in enumerate(checkpoints, 1):
        logger.info(
            "  Run %2d  end=%-12s  run_id=%s",
            i,
            str(d.date()),
            _run_id(d),
        )

    if args.dry_run:
        logger.info("Dry-run mode — no training performed.")
        return

    # ── Reset registry ───────────────────────────────────────────────────────
    registry_path = args.output_dir / "sarima_metadata.json"
    if registry_path.exists():
        registry_path.unlink()
        logger.info("Existing registry removed — will be rebuilt from scratch.")

    order = (_ts.DEFAULT_P, _ts.DEFAULT_D, _ts.DEFAULT_Q)
    seasonal_order = (_ts.DEFAULT_P_S, _ts.DEFAULT_D_S, _ts.DEFAULT_Q_S, _ts.DEFAULT_M)

    # ── Training loop ─────────────────────────────────────────────────────────
    for i, training_end in enumerate(checkpoints, 1):
        run_id = _run_id(training_end)
        sim_trained_at = _trained_at(training_end)
        train_end_str = training_end.strftime("%Y-%m-%d")
        run_dir = args.output_dir / run_id

        logger.info("=" * 62)
        logger.info(
            "[%d/%d] run_id=%s  window=…→%s",
            i,
            len(checkpoints),
            run_id,
            train_end_str,
        )
        logger.info("=" * 62)

        # ── Resume path: re-register from existing artefacts ────────────────
        if args.skip_existing and run_dir.exists():
            run_meta_path = run_dir / f"sarima_run_{run_id}.json"
            if run_meta_path.exists():
                with run_meta_path.open(encoding="utf-8") as fh:
                    saved = json.load(fh)
                meta = saved.get("model", {})
                meta["trained_at"] = sim_trained_at
                _update_registry(args.output_dir, run_id, meta)
                logger.info("Skipped (existing) — re-registered in registry.")
                continue
            else:
                logger.warning(
                    "Directory exists but no run JSON found — will re-train: %s", run_dir
                )

        run_dir.mkdir(parents=True, exist_ok=True)

        # 1. Load data up to training_end (full history from the start)
        ts_series, exog_all, scale_columns = load_and_prepare(
            args.silver_path,
            train_start=None,
            train_end=train_end_str,
        )

        # 2. Select features
        exog_df = select_features(exog_all, DEFAULT_FEATURES)

        # 3. Fit model
        model_result, scaler, exog_columns = fit_model(
            ts=ts_series,
            exog_df=exog_df,
            order=order,
            seasonal_order=seasonal_order,
            scale_cols=scale_columns,
        )

        # 4. In-sample metrics
        metrics = compute_insample_metrics(model_result, ts_series)
        logger.info("In-sample metrics: %s", metrics)

        # 5. Save artefacts (trained_at will be overridden with simulated date)
        meta = save_artefacts(
            output_dir=run_dir,
            run_id=run_id,
            model_result=model_result,
            scaler=scaler,
            exog_columns=exog_columns,
            order=order,
            seasonal_order=seasonal_order,
            ts=ts_series,
            train_start=None,
            train_end=train_end_str,
            metrics=metrics,
        )

        # 5b. Copy PCA artefacts
        copy_pca_artefacts(args.silver_path, run_dir, meta)

        # 6. Override trained_at with the simulated date
        meta["trained_at"] = sim_trained_at

        # 7. Write per-run metadata JSON
        run_meta_path = run_dir / f"sarima_run_{run_id}.json"
        with run_meta_path.open("w", encoding="utf-8") as fh:
            json.dump({"run_id": run_id, "model": meta}, fh, indent=2, ensure_ascii=False)
        logger.info("Run metadata written → %s", run_meta_path)

        # 8. Write human-readable training report
        _write_training_report(
            output_dir=run_dir,
            run_id=run_id,
            order=order,
            seasonal_order=seasonal_order,
            features=DEFAULT_FEATURES,
            train_start=None,
            train_end=train_end_str,
            meta=meta,
            model_result=model_result,
        )

        # 9. Update cumulative registry
        _update_registry(args.output_dir, run_id, meta)
        logger.info("Run %s complete.\n", run_id)

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("=" * 62)
    logger.info("Simulation complete — %d runs registered.", len(checkpoints))
    if registry_path.exists():
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
        logger.info("Latest run : %s", registry.get("latest_run_id"))
        logger.info("Total runs : %s", registry.get("n_runs"))
    logger.info("Registry   : %s", registry_path)
    logger.info("=" * 62)


if __name__ == "__main__":
    main()
