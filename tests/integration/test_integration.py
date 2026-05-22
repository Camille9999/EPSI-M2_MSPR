import sys
from importlib import import_module
from pathlib import Path

import pandas as pd


SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "src" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


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
