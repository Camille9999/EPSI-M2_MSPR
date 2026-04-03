import argparse
import logging
import re
from pathlib import Path

import pandas as pd

from utils.load_env import PATH_DATA


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

STATION_ID_REGEX = re.compile(r"(\d{9})(?=\.csv$)")


def extract_station_id(file_path: Path) -> str:
    """Extract 9-digit station identifier from SQR filename."""
    match = STATION_ID_REGEX.search(file_path.name)
    if not match:
        raise ValueError(f"Cannot extract station id from file name: {file_path.name}")
    return match.group(1)


def load_station_series(file_path: Path, value_prefix: str) -> pd.Series:
    """Load one station CSV file and return a daily Series named with station id."""
    station_id = extract_station_id(file_path)
    output_column = f"{value_prefix}_{station_id}"

    df = pd.read_csv(
        file_path,
        sep=";",
        skiprows=9,
        usecols=["AAAAMMJJ", "VALEUR"],
        dtype={"AAAAMMJJ": "string", "VALEUR": "float64"},
        na_values=["", "mq", "MQ"],
    )

    df["AAAAMMJJ"] = pd.to_datetime(df["AAAAMMJJ"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["AAAAMMJJ"])
    df = df.drop_duplicates(subset=["AAAAMMJJ"], keep="last")
    df = df.sort_values("AAAAMMJJ").set_index("AAAAMMJJ")

    return df["VALEUR"].rename(output_column)


def load_folder_as_wide_dataframe(folder_path: Path, value_prefix: str, glob_pattern: str) -> pd.DataFrame:
    """Load all station files from one folder into a wide dataframe indexed by day."""
    csv_files = sorted(folder_path.glob(glob_pattern))
    if not csv_files:
        raise FileNotFoundError(f"No files matching '{glob_pattern}' found in {folder_path}")

    logger.info("Loading %s files from %s", len(csv_files), folder_path)

    series_list: list[pd.Series] = []
    failed_files = 0

    for file_path in csv_files:
        try:
            series_list.append(load_station_series(file_path, value_prefix=value_prefix))
        except Exception as error:
            failed_files += 1
            logger.warning("Skipping file %s due to error: %s", file_path.name, error)

    if not series_list:
        raise RuntimeError(f"All files failed for {folder_path}")

    df = pd.concat(series_list, axis=1)
    df.index.name = "date"
    df = df.sort_index()

    logger.info(
        "Built dataframe for %s with shape=%s (failed files=%s)",
        value_prefix,
        df.shape,
        failed_files,
    )

    return df


def aggregate_sqr_to_bronze(tn_dir: Path, tx_dir: Path, output_path: Path) -> Path:
    """Aggregate SQR Tmin/Tmax station files into one daily parquet table."""
    tn_df = load_folder_as_wide_dataframe(
        folder_path=tn_dir,
        value_prefix="TN",
        glob_pattern="SQR_MTN*.csv",
    )
    tx_df = load_folder_as_wide_dataframe(
        folder_path=tx_dir,
        value_prefix="TX",
        glob_pattern="SQR_MTX*.csv",
    )

    logger.info("Merging TN and TX dataframes")
    merged_df = pd.concat([tn_df, tx_df], axis=1).sort_index()

    # Float32 significantly reduces parquet size while keeping enough precision for temperature data.
    merged_df = merged_df.astype("float32")

    final_df = merged_df.reset_index()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Writing parquet to %s", output_path)
    final_df.to_parquet(output_path, index=False, compression="snappy")

    logger.info("Bronze dataset created: %s", output_path)
    logger.info("Rows (days): %s", len(final_df))
    logger.info("Columns: %s", len(final_df.columns))

    return output_path


def build_default_paths() -> tuple[Path, Path, Path]:
    if not PATH_DATA:
        raise ValueError("PATH_DATA is not configured in environment")

    base_path = Path(PATH_DATA)
    tn_dir = base_path / "source" / "sqr_tn_metro"
    tx_dir = base_path / "source" / "sqr_tx_metro"
    output_path = base_path / "bronze" / "sqr_daily_by_station.parquet"
    return tn_dir, tx_dir, output_path


def parse_args() -> argparse.Namespace:
    default_tn_dir, default_tx_dir, default_output = build_default_paths()

    parser = argparse.ArgumentParser(
        description=(
            "Aggregate SQR TN/TX station csv files into a daily bronze parquet dataset "
            "with one row per day and columns TN_<station> / TX_<station>."
        )
    )
    parser.add_argument("--tn-dir", type=Path, default=default_tn_dir, help="Folder containing SQR_MTN*.csv")
    parser.add_argument("--tx-dir", type=Path, default=default_tx_dir, help="Folder containing SQR_MTX*.csv")
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help="Output parquet file path",
    )
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()
        aggregate_sqr_to_bronze(
            tn_dir=args.tn_dir,
            tx_dir=args.tx_dir,
            output_path=args.output,
        )
        return 0
    except Exception as error:
        logger.error("Aggregation failed: %s", error, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
