import json
import sys
from importlib import import_module
from pathlib import Path

import joblib
import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "src" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


class FakeApiModel:
    def forecast(self, steps, exog):
        return pd.Series([1000.0 + float(exog[0, -1])])


class FakeApiScaler:
    def transform(self, values):
        return values


def _write_rte_raw_file(path: Path) -> None:
    rows = [
        ["Nature", "Consommation", "Prévision J-1", "Prévision J", "Fioul", "Charbon", "Gaz", "Nucléaire", "Eolien", "Solaire", "Hydraulique", "Pompage", "Bioénergies", "Ech. physiques"],
        ["2024-01-01", "100", "98", "99", "1", "2", "3", "70", "4", "5", "6", "0", "1", "8"],
        ["2024-01-02", "110", "108", "109", "1", "2", "3", "75", "4", "5", "6", "0", "1", "8"],
        ["2024-01-03", "120", "118", "119", "1", "2", "3", "80", "4", "5", "6", "0", "1", "8"],
        ["2024-01-04", "130", "128", "129", "1", "2", "3", "85", "4", "5", "6", "0", "1", "8"],
        ["2024-01-05", "140", "138", "139", "1", "2", "3", "90", "4", "5", "6", "0", "1", "8"],
    ]
    path.write_text("\n".join("\t".join(row) for row in rows), encoding="utf-8")


def _write_sqr_raw_file(path: Path, values: list[float]) -> None:
    metadata = [f"metadata line {index}" for index in range(1, 10)]
    rows = ["AAAAMMJJ;VALEUR"]
    for day, value in enumerate(values, start=1):
        rows.append(f"2024010{day};{value}")

    path.write_text("\n".join([*metadata, *rows]), encoding="utf-8")


def _write_api_model_registry(models_dir: Path) -> str:
    run_id = "test_run_20240101"
    run_dir = models_dir / run_id
    run_dir.mkdir(parents=True)

    joblib.dump(FakeApiModel(), run_dir / "model.pkl")
    joblib.dump(FakeApiScaler(), run_dir / "scaler.pkl")

    model_metadata = {
        "run_id": run_id,
        "model": {
            "features": ["temp_pc_01", "production_mw_lag1"],
            "scale_columns": ["temp_pc_01", "production_mw_lag1"],
            "target_column": "consommation_mw",
            "order": [1, 0, 0],
            "seasonal_order": [0, 0, 0, 0],
            "training_start": "2024-01-01",
            "training_end": "2024-01-05",
            "n_training_days": 5,
        },
    }
    (run_dir / "metadata.json").write_text(
        json.dumps(model_metadata),
        encoding="utf-8",
    )

    registry = {
        "latest_run_id": run_id,
        "n_runs": 1,
        "runs": [
            {
                "run_id": run_id,
                "run_dir": run_id,
                "trained_at": "2024-01-06T00:00:00Z",
                "training_start": "2024-01-01",
                "training_end": "2024-01-05",
                "n_training_days": 5,
                "features": ["temp_pc_01", "production_mw_lag1"],
                "order": [1, 0, 0],
                "seasonal_order": [0, 0, 0, 0],
                "model": {
                    "model_file": "model.pkl",
                    "scaler_file": "scaler.pkl",
                    "metadata_file": "metadata.json",
                    "insample_metrics": {
                        "insample_MAE_MW": 1.2,
                        "insample_RMSE_MW": 1.5,
                        "insample_MAPE_pct": 0.1,
                    },
                },
            }
        ],
    }
    (models_dir / "sarima_metadata.json").write_text(
        json.dumps(registry),
        encoding="utf-8",
    )
    return run_id


def test_raw_to_bronze_to_silver_to_training_input_pipeline(tmp_path):
    raw_to_bronze_rte = import_module("raw_to_bronze_rte")
    raw_to_bronze_sqr = import_module("raw_to_bronze_sqr")
    bronze_to_silver = import_module("bronze_to_silver")
    train_sarima = import_module("train_sarima")

    data_dir = tmp_path / "data"
    rte_source_dir = data_dir / "source" / "RTE"
    sqr_tn_dir = data_dir / "source" / "sqr_tn_metro"
    sqr_tx_dir = data_dir / "source" / "sqr_tx_metro"
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"

    rte_source_dir.mkdir(parents=True)
    sqr_tn_dir.mkdir(parents=True)
    sqr_tx_dir.mkdir(parents=True)

    _write_rte_raw_file(rte_source_dir / "eCO2mix_RTE_Annuel-Definitif_2024.xls")
    _write_sqr_raw_file(sqr_tn_dir / "SQR_MTN_000000001.csv", [1.0, 2.0, 3.0, 4.0, 5.0])
    _write_sqr_raw_file(sqr_tn_dir / "SQR_MTN_000000002.csv", [2.0, 3.0, 4.0, 5.0, 6.0])
    _write_sqr_raw_file(sqr_tx_dir / "SQR_MTX_000000003.csv", [10.0, 11.0, 12.0, 13.0, 14.0])
    _write_sqr_raw_file(sqr_tx_dir / "SQR_MTX_000000004.csv", [11.0, 12.0, 13.0, 14.0, 15.0])

    rte_bronze_path = bronze_dir / "rte_annuel_definitif.parquet"
    sqr_bronze_path = bronze_dir / "sqr_daily_by_station.parquet"
    silver_path = silver_dir / "rte_sqr_daily_silver.parquet"

    raw_to_bronze_rte.aggregate_rte_files_to_bronze(rte_source_dir, rte_bronze_path)
    raw_to_bronze_sqr.aggregate_sqr_to_bronze(sqr_tn_dir, sqr_tx_dir, sqr_bronze_path)
    bronze_to_silver.build_silver_dataset(rte_bronze_path, sqr_bronze_path, silver_path, n_components=3)

    silver_df = pd.read_parquet(silver_path)
    assert list(silver_df.columns) == [
        "date",
        "consommation_mw",
        "prevision_j1_mw",
        "prevision_j_mw",
        "production_mw",
        "temp_pc_01",
        "temp_pc_02",
        "temp_pc_03",
    ]
    assert len(silver_df) == 5

    ts, exog_all, scale_columns = train_sarima.load_and_prepare(silver_path)
    exog_df = train_sarima.select_features(exog_all, train_sarima.DEFAULT_FEATURES)

    assert len(ts) == 4
    assert list(exog_df.columns) == train_sarima.DEFAULT_FEATURES
    assert "production_mw_lag1" in exog_all.columns
    assert scale_columns == train_sarima.DEFAULT_FEATURES


def test_api_serves_registered_model_metadata_and_predictions(tmp_path, monkeypatch):
    from src.api import main as api_main

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    run_id = _write_api_model_registry(models_dir)

    monkeypatch.setattr(api_main, "MODEL_DIR", models_dir)
    monkeypatch.setattr(api_main, "REGISTRY_PATH", models_dir / "sarima_metadata.json")
    api_main._cache.clear()
    api_main._latest_run_id = None

    api_main._startup_load()

    assert api_main.health().model_dump() == {
        "status": "ok",
        "model_loaded": True,
        "latest_run_id": run_id,
        "model_name": "SARIMAX(1, 0, 0)x(0, 0, 0, 0)",
    }

    runs = [run.model_dump() for run in api_main.list_runs()]
    assert runs[0]["run_id"] == run_id
    assert runs[0]["mae_mw"] == 1.2

    metadata = api_main.get_metadata().model_dump()
    assert metadata["run_id"] == run_id
    assert metadata["target_name"] == "consommation_mw"
    assert metadata["user_inputs"] == [
        "temp_min_avg",
        "temp_max_avg",
        "production_mw_lag1",
    ]
    assert metadata["internal_features"] == ["temp_pc_01", "production_mw_lag1"]
    assert metadata["pca_available"] is False

    prediction = api_main.predict(
        api_main.PredictRequest(
            temp_min_avg=4.0,
            temp_max_avg=12.0,
            production_mw_lag1=250.0,
        )
    ).model_dump()
    assert prediction["run_id"] == run_id
    assert prediction["prediction"] == 1250.0
    assert prediction["pca_components"] == []


def test_api_returns_404_for_unknown_run_id(tmp_path, monkeypatch):
    from src.api import main as api_main

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    _write_api_model_registry(models_dir)

    monkeypatch.setattr(api_main, "MODEL_DIR", models_dir)
    monkeypatch.setattr(api_main, "REGISTRY_PATH", models_dir / "sarima_metadata.json")
    api_main._cache.clear()
    api_main._latest_run_id = None

    api_main._startup_load()

    with pytest.raises(api_main.HTTPException) as exc_info:
        api_main.get_metadata(run_id="unknown_run")

    assert exc_info.value.status_code == 404
