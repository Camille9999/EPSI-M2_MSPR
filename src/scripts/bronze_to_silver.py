import argparse
import json
import logging
from pathlib import Path

import joblib
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from utils.load_env import PATH_DATA


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

RTE_DAILY_MEAN_COLUMNS = ["Consommation", "Prévision J-1", "Prévision J"]
RTE_PRODUCTION_COLUMNS = [
    "Fioul",
    "Charbon",
    "Gaz",
    "Nucléaire",
    "Eolien",
    "Solaire",
    "Hydraulique",
    "Pompage",
    "Bioénergies",
    "Ech. physiques",
]


def build_default_paths() -> tuple[Path, Path, Path]:
    if not PATH_DATA:
        raise ValueError("PATH_DATA is not configured in environment")

    base_path = Path(PATH_DATA)
    rte_bronze_path = base_path / "bronze" / "rte_annuel_definitif.parquet"
    sqr_bronze_path = base_path / "bronze" / "sqr_daily_by_station.parquet"
    silver_output_path = base_path / "silver" / "rte_sqr_daily_silver.parquet"
    return rte_bronze_path, sqr_bronze_path, silver_output_path


def parse_args() -> argparse.Namespace:
    default_rte_path, default_sqr_path, default_output_path = build_default_paths()

    parser = argparse.ArgumentParser(
        description=(
            "Build one joined silver dataset from bronze RTE and SQR data. "
            "RTE is aggregated daily with mean MW values; SQR is reduced with one PCA using 3 components."
        )
    )
    parser.add_argument(
        "--rte-bronze",
        type=Path,
        default=default_rte_path,
        help="Input bronze parquet for RTE",
    )
    parser.add_argument(
        "--sqr-bronze",
        type=Path,
        default=default_sqr_path,
        help="Input bronze parquet for SQR",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output_path,
        help="Output silver parquet path",
    )
    parser.add_argument(
        "--pca-components",
        type=int,
        default=3,
        help="Number of PCA components to keep for SQR temperature features",
    )
    return parser.parse_args()


def require_columns(df: pd.DataFrame, required_columns: list[str], dataset_name: str) -> None:
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in {dataset_name}: {missing_columns}")


def load_parquet(path: Path, dataset_name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{dataset_name} file not found: {path}")

    logger.info("Loading %s from %s", dataset_name, path)
    return pd.read_parquet(path)


def prepare_rte_daily_features(rte_df: pd.DataFrame) -> pd.DataFrame:
    required_columns = ["Nature", *RTE_DAILY_MEAN_COLUMNS, *RTE_PRODUCTION_COLUMNS]
    require_columns(rte_df, required_columns, "RTE bronze")

    working_df = rte_df.copy()
    working_df["date"] = pd.to_datetime(working_df["Nature"], errors="coerce")
    working_df = working_df.dropna(subset=["date"]).copy()

    numeric_columns = [*RTE_DAILY_MEAN_COLUMNS, *RTE_PRODUCTION_COLUMNS]
    for column in numeric_columns:
        working_df[column] = pd.to_numeric(working_df[column], errors="coerce")

    working_df["Production"] = working_df[RTE_PRODUCTION_COLUMNS].sum(axis=1, min_count=1)

    daily_rte = (
        working_df.groupby("date", as_index=False)
        .agg(
            consommation_mw=("Consommation", "mean"),
            prevision_j1_mw=("Prévision J-1", "mean"),
            prevision_j_mw=("Prévision J", "mean"),
            production_mw=("Production", "mean"),
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    return daily_rte


def build_pca_pipeline(n_components: int) -> Pipeline:
    return Pipeline(
        memory=None,
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("pca", PCA(n_components=n_components, random_state=42)),
        ],
    )


def prepare_sqr_pca_features(sqr_df: pd.DataFrame, rte_dates: pd.Series, n_components: int) -> pd.DataFrame:
    require_columns(sqr_df, ["date"], "SQR bronze")

    working_df = sqr_df.copy()
    working_df["date"] = pd.to_datetime(working_df["date"], errors="coerce")
    working_df = working_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    temperature_columns = [
        column
        for column in working_df.columns
        if column.startswith("TN_") or column.startswith("TX_")
    ]
    if not temperature_columns:
        raise ValueError("No SQR temperature columns found with TN_/TX_ prefixes")

    rte_start = rte_dates.min()
    rte_end = rte_dates.max()
    sqr_common = working_df.loc[working_df["date"].between(rte_start, rte_end)].copy()
    if sqr_common.empty:
        raise ValueError("No overlapping SQR dates found within RTE coverage")

    pca_input = sqr_common[["date", *temperature_columns]].copy().set_index("date")
    pca_pipeline = build_pca_pipeline(n_components=n_components)
    pca_scores = pca_pipeline.fit_transform(pca_input)

    explained_variance = pca_pipeline.named_steps["pca"].explained_variance_ratio_
    logger.info("SQR PCA explained variance ratio: %s", [round(value, 6) for value in explained_variance])
    logger.info("SQR PCA cumulative explained variance: %.6f", explained_variance.sum())

    pca_feature_names = [f"temp_pc_{index:02d}" for index in range(1, n_components + 1)]
    sqr_pca_features = pd.DataFrame(
        pca_scores,
        index=pca_input.index,
        columns=pca_feature_names,
    ).reset_index()

    return sqr_pca_features, pca_pipeline, temperature_columns


def build_silver_dataset(rte_bronze_path: Path, sqr_bronze_path: Path, output_path: Path, n_components: int) -> Path:
    rte_bronze = load_parquet(rte_bronze_path, "RTE bronze")
    sqr_bronze = load_parquet(sqr_bronze_path, "SQR bronze")

    daily_rte = prepare_rte_daily_features(rte_bronze)
    sqr_pca_features, pca_pipeline, pca_columns = prepare_sqr_pca_features(
        sqr_bronze, daily_rte["date"], n_components=n_components
    )

    silver_df = (
        daily_rte.merge(sqr_pca_features, on="date", how="inner", validate="one_to_one")
        .sort_values("date")
        .reset_index(drop=True)
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    silver_df.to_parquet(output_path, index=False, compression="snappy")

    # Persist the fitted PCA pipeline so downstream consumers (API) can apply it at inference.
    pca_dir = output_path.parent
    pca_pipeline_path = pca_dir / "pca_pipeline.pkl"
    pca_columns_path = pca_dir / "pca_columns.json"
    joblib.dump(pca_pipeline, pca_pipeline_path)
    pca_columns_path.write_text(json.dumps(pca_columns), encoding="utf-8")

    logger.info("Silver dataset written to: %s", output_path)
    logger.info("Silver shape: %s", silver_df.shape)
    logger.info("Silver columns: %s", list(silver_df.columns))
    logger.info("PCA pipeline saved to: %s", pca_pipeline_path)
    logger.info("PCA columns saved to:  %s", pca_columns_path)

    return output_path


def main() -> int:
    try:
        args = parse_args()
        build_silver_dataset(
            rte_bronze_path=args.rte_bronze,
            sqr_bronze_path=args.sqr_bronze,
            output_path=args.output,
            n_components=args.pca_components,
        )
        return 0
    except Exception as error:
        logger.error("Silver dataset build failed: %s", error, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
