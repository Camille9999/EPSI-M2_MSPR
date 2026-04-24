import argparse
import logging
import re
from pathlib import Path

import csv

import pandas as pd

from utils.load_env import PATH_DATA


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

YEAR_REGEX = re.compile(r"(\d{4})(?=\.xls$)")


def extract_year_from_filename(file_path: Path) -> int:
    match = YEAR_REGEX.search(file_path.name)
    if not match:
        raise ValueError(f"Unable to extract year from file name: {file_path.name}")
    return int(match.group(1))


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names by trimming spaces and new lines."""
    df = df.copy()
    df.columns = [str(col).replace("\n", " ").strip() for col in df.columns]
    return df


def load_rte_as_text_table(file_path: Path) -> pd.DataFrame:
    """Load RTE files that are tab-separated text despite .xls extension."""
    last_error = None
    encodings_to_try = ["utf-8-sig", "cp1252", "latin1"]

    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(
                file_path,
                sep="\t",
                encoding=encoding,
                engine="python",
                dtype="string",
                quoting=csv.QUOTE_MINIMAL,
                na_values=["", "ND", "N/A", "NA"],
                keep_default_na=True,
            )
            if len(df.columns) > 1:
                logger.info("Loaded %s as tabular text with encoding=%s", file_path.name, encoding)
                return df
        except Exception as error:
            last_error = error

    raise RuntimeError(
        f"Unable to read text-based table from {file_path.name}. "
        f"Last error: {last_error}"
    )


def coerce_mostly_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert object/string columns to numeric when content is mostly numeric."""
    df = df.copy()

    for column in df.columns:
        series = df[column]
        if not pd.api.types.is_string_dtype(series) and not pd.api.types.is_object_dtype(series):
            continue

        normalized = (
            series.astype("string")
            .str.replace(",", ".", regex=False)
            .str.strip()
        )
        numeric_candidate = pd.to_numeric(normalized, errors="coerce")

        non_null_count = int(series.notna().sum())
        if non_null_count == 0:
            continue

        converted_ratio = float(numeric_candidate.notna().sum() / non_null_count)
        if converted_ratio >= 0.8:
            df[column] = numeric_candidate

    return df


def has_alternating_missing_pattern(series: pd.Series, min_ratio: float = 0.35, max_ratio: float = 0.65) -> bool:
    """Detect one-missing-out-of-two pattern in a column."""
    mask = series.isna()
    ratio = float(mask.mean())

    if ratio < min_ratio or ratio > max_ratio:
        return False
    if len(mask) < 4:
        return False

    even_missing_ratio = float(mask.iloc[::2].mean())
    odd_missing_ratio = float(mask.iloc[1::2].mean())

    # Alternating patterns: [missing, value, missing, value] or [value, missing, value, missing]
    return (
        even_missing_ratio >= 0.9 and odd_missing_ratio <= 0.1
    ) or (
        odd_missing_ratio >= 0.9 and even_missing_ratio <= 0.1
    )


def backfill_sparse_alternating_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Backfill columns that look like alternating structural gaps."""
    df = df.copy()
    columns_to_bfill: list[str] = []

    for column in df.columns:
        if has_alternating_missing_pattern(df[column]):
            columns_to_bfill.append(column)

    if columns_to_bfill:
        logger.info("Backfill applied on %s columns with alternating missing pattern", len(columns_to_bfill))
        logger.info("Columns: %s", columns_to_bfill)
        df[columns_to_bfill] = df[columns_to_bfill].bfill()
    else:
        logger.info("No alternating missing pattern detected")

    return df


def build_datetime_column(df: pd.DataFrame) -> pd.DataFrame:
    """Build a timestamp column when date/time columns are available."""
    df = df.copy()

    lowered = {col.lower(): col for col in df.columns}
    date_col = next((col for low, col in lowered.items() if "date" in low), None)
    hour_col = next((col for low, col in lowered.items() if "heure" in low or "hour" in low), None)

    if date_col is None:
        return df

    date_values = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)

    if hour_col is None:
        df["timestamp"] = date_values
        return df

    hour_as_text = df[hour_col].astype("string").str.strip()
    timestamp = pd.to_datetime(
        date_values.dt.strftime("%Y-%m-%d") + " " + hour_as_text,
        errors="coerce",
    )
    df["timestamp"] = timestamp
    return df


def load_rte_year_file(file_path: Path) -> pd.DataFrame:
    """Load one annual definitive RTE XLS file."""
    year = extract_year_from_filename(file_path)
    logger.info("Loading file: %s", file_path.name)

    try:
        df = pd.read_excel(file_path, engine="xlrd")
    except Exception as error:
        logger.info(
            "Excel binary read failed for %s (%s). Trying text-table fallback.",
            file_path.name,
            error,
        )
        df = load_rte_as_text_table(file_path)

    df = normalize_columns(df)

    # Remove empty rows/columns created by XLS layout artifacts.
    df = df.dropna(axis=0, how="all")
    df = df.dropna(axis=1, how="all")

    df = coerce_mostly_numeric_columns(df)

    df = backfill_sparse_alternating_columns(df)
    df = build_datetime_column(df)
    df["source_year"] = year

    return df


def aggregate_rte_files_to_bronze(source_dir: Path, output_path: Path) -> Path:
    """Concatenate all annual definitive RTE files and write one parquet file."""
    input_files = sorted(source_dir.glob("eCO2mix_RTE_Annuel-Definitif_*.xls"))
    if not input_files:
        raise FileNotFoundError(f"No RTE annual definitive file found in {source_dir}")

    yearly_frames: list[pd.DataFrame] = []

    for file_path in input_files:
        try:
            yearly_frames.append(load_rte_year_file(file_path))
        except Exception as error:
            logger.warning("Skipping %s due to error: %s", file_path.name, error)

    if not yearly_frames:
        raise RuntimeError("No yearly RTE files could be loaded")

    logger.info("Concatenating %s yearly datasets", len(yearly_frames))
    merged_df = pd.concat(yearly_frames, axis=0, ignore_index=True)

    if "timestamp" in merged_df.columns:
        merged_df = merged_df.sort_values(["timestamp", "source_year"], na_position="last")
    else:
        merged_df = merged_df.sort_values(["source_year"])  # Fallback sort

    merged_df = merged_df.drop_duplicates(ignore_index=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_parquet(output_path, index=False, compression="snappy")

    logger.info("Bronze parquet written to: %s", output_path)
    logger.info("Final shape: %s", merged_df.shape)

    return output_path


def build_default_paths() -> tuple[Path, Path]:
    if not PATH_DATA:
        raise ValueError("PATH_DATA is not configured in environment")

    base_path = Path(PATH_DATA)
    source_dir = base_path / "source" / "RTE"
    output_path = base_path / "bronze" / "rte_annuel_definitif.parquet"
    return source_dir, output_path


def parse_args() -> argparse.Namespace:
    default_source_dir, default_output = build_default_paths()

    parser = argparse.ArgumentParser(
        description=(
            "Aggregate yearly RTE annual definitive XLS files into one bronze parquet dataset, "
            "with backfill on alternating-missing columns."
        )
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=default_source_dir,
        help="Folder containing eCO2mix_RTE_Annuel-Definitif_*.xls files",
    )
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
        aggregate_rte_files_to_bronze(
            source_dir=args.source_dir,
            output_path=args.output,
        )
        return 0
    except Exception as error:
        logger.error("RTE bronze aggregation failed: %s", error, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
