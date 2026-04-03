import logging
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.load_env import PATH_DATA, SQR_TX_METRO, SQR_TN_METRO


logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


DOWNLOAD_TARGETS = {
	"sqr_tx_metro": SQR_TX_METRO,
	"sqr_tn_metro": SQR_TN_METRO,
}

EXPECTED_FILES_PER_ARCHIVE = 198


def create_base_source_directory() -> Path:
	"""Create PATH_DATA/source directory if it doesn't exist."""
	source_dir = Path(PATH_DATA) / "source"
	source_dir.mkdir(parents=True, exist_ok=True)
	logger.info("Source directory ready: %s", source_dir)
	return source_dir


def download_file(url: str, dest_path: Path) -> bool:
	"""Download a single file from URL to destination path."""
	try:
		logger.info("Downloading: %s", url)
		with urlopen(url) as response:
			content_type = (response.headers.get("content-type") or "").lower()
			if "text/html" in content_type:
				logger.error("URL did not return a file (content-type=%s): %s", content_type, url)
				return False

			total_size = response.headers.get("content-length")
			if total_size:
				total_size = int(total_size)
				logger.info("File size: %.2f MB", total_size / (1024**2))

			with open(dest_path, "wb") as out_file:
				chunk_size = 8192
				while True:
					chunk = response.read(chunk_size)
					if not chunk:
						break
					out_file.write(chunk)

		logger.info("Successfully downloaded to: %s", dest_path)
		if not zipfile.is_zipfile(dest_path):
			logger.error("Downloaded file is not a valid zip archive: %s", dest_path)
			return False

		return True
	except Exception as error:
		logger.error("Error downloading %s: %s", url, error)
		return False


def resolve_data_gouv_download_url(url: str) -> str:
	"""Resolve data.gouv page URLs to direct downloadable resource URLs."""
	parsed = urlparse(url)
	resource_id = parse_qs(parsed.query).get("resource_id", [None])[0]
	if resource_id:
		resolved_url = f"https://www.data.gouv.fr/fr/datasets/r/{resource_id}"
		logger.info("Resolved Data.gouv URL to resource endpoint: %s", resolved_url)
		return resolved_url
	return url


def extract_zip(zip_path: Path, target_dir: Path) -> bool:
	"""Extract all files from zip to target directory."""
	try:
		logger.info("Extracting %s into %s", zip_path.name, target_dir)
		with zipfile.ZipFile(zip_path, "r") as zip_ref:
			files_extracted = 0
			for file_info in zip_ref.filelist:
				if file_info.filename.endswith("/"):
					continue

				# Flatten nested zip paths and keep only the filename.
				file_name = Path(file_info.filename).name
				if not file_name:
					continue

				file_content = zip_ref.read(file_info.filename)
				output_path = target_dir / file_name
				output_path.write_bytes(file_content)
				files_extracted += 1

		if files_extracted == 0:
			logger.warning("No files extracted from %s", zip_path)
			return False

		logger.info("Successfully extracted %s files", files_extracted)
		return True
	except Exception as error:
		logger.error("Error extracting %s: %s", zip_path, error, exc_info=True)
		return False


def get_zip_name_from_url(url: str, fallback_name: str) -> str:
	"""Get filename from URL path, or use fallback if missing."""
	parsed_url = urlparse(url)
	file_name = Path(parsed_url.path).name
	if file_name:
		return file_name
	return fallback_name


def process_target(target_name: str, url: str, base_source_dir: Path) -> dict:
	"""Download and extract one SQR zip file into its dedicated folder."""
	if not url:
		logger.error("Missing URL for %s in environment", target_name)
		return {"target": target_name, "success": False, "reason": "missing_url"}

	target_dir = base_source_dir / target_name
	target_dir.mkdir(parents=True, exist_ok=True)
	data_url = resolve_data_gouv_download_url(url)

	existing_files = [file for file in target_dir.glob("*") if file.is_file()]
	existing_data_files = [file for file in existing_files if file.suffix.lower() != ".zip"]
	if len(existing_data_files) >= EXPECTED_FILES_PER_ARCHIVE:
		logger.info(
			"Data already present for %s (%s files), skipping",
			target_name,
			len(existing_data_files),
		)
		return {"target": target_name, "success": True, "reason": "already_exists"}
	if existing_data_files:
		logger.warning(
			"Incomplete data detected for %s (%s/%s files). Cleaning directory and retrying.",
			target_name,
			len(existing_data_files),
			EXPECTED_FILES_PER_ARCHIVE,
		)
		for file_path in existing_files:
			try:
				file_path.unlink()
			except Exception as error:
				logger.warning("Could not remove stale file %s: %s", file_path, error)

	zip_name = get_zip_name_from_url(data_url, f"{target_name}.zip")
	if not zip_name.lower().endswith(".zip"):
		zip_name = f"{zip_name}.zip"
	zip_path = target_dir / zip_name

	if not download_file(data_url, zip_path):
		return {"target": target_name, "success": False, "reason": "download_failed"}

	if not extract_zip(zip_path, target_dir):
		return {"target": target_name, "success": False, "reason": "extraction_failed"}

	try:
		zip_path.unlink()
		logger.info("Removed zip file: %s", zip_path)
	except Exception as error:
		logger.warning("Could not remove zip file %s: %s", zip_path, error)

	extracted_files = [file for file in target_dir.glob("*") if file.is_file()]
	logger.info("%s: %s files available in %s", target_name, len(extracted_files), target_dir)
	return {"target": target_name, "success": True, "reason": "extracted"}


def main() -> int:
	"""Download and extract SQR temperature datasets."""
	base_source_dir = create_base_source_directory()

	successful = 0
	failed = 0
	results = []

	active_targets = {name: url for name, url in DOWNLOAD_TARGETS.items() if url}
	missing_targets = [name for name, url in DOWNLOAD_TARGETS.items() if not url]

	if missing_targets:
		for target_name in missing_targets:
			logger.warning("Environment variable missing for target: %s", target_name)

	if not active_targets:
		logger.error("No valid SQR URLs configured (SQR_TX_METRO / SQR_TN_METRO)")
		return 1

	max_workers = min(4, len(active_targets))
	logger.info("Starting downloads with %s workers", max_workers)

	with ThreadPoolExecutor(max_workers=max_workers) as executor:
		futures = {
			executor.submit(process_target, target_name, url, base_source_dir): target_name
			for target_name, url in active_targets.items()
		}

		for future in as_completed(futures):
			try:
				result = future.result()
				results.append(result)
				if result["success"]:
					successful += 1
					logger.info("[OK] %s: %s", result["target"], result["reason"])
				else:
					failed += 1
					logger.error("[KO] %s: %s", result["target"], result["reason"])
			except Exception as error:
				failed += 1
				logger.error("Unexpected processing error: %s", error)

	logger.info("=" * 60)
	logger.info("SQR Download Summary")
	logger.info("  Successful: %s", successful)
	logger.info("  Failed: %s", failed)
	logger.info("  Total targets: %s", len(active_targets))
	logger.info("  Base directory: %s", base_source_dir)
	logger.info("=" * 60)

	return 0 if failed == 0 else 1


if __name__ == "__main__":
	raise SystemExit(main())
