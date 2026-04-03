import logging
from pathlib import Path
from urllib.request import urlopen
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.load_env import PATH_DATA, DATA_ANN_DEF_YR_START, DATA_ANN_DEF_YR_END

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

URLS = [
    f"https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Annuel-Definitif_{year}.zip"
    for year in range(DATA_ANN_DEF_YR_START, DATA_ANN_DEF_YR_END + 1)
]


def create_source_directory():
    """Create PATH_DATA/source/RTE directory if it doesn't exist."""
    source_dir = Path(PATH_DATA) / "source" / "RTE"
    source_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Source directory ready: {source_dir}")
    return source_dir


def download_file(url, dest_path):
    """Download a single file from URL to destination path."""
    try:
        logger.info(f"Downloading: {url}")
        with urlopen(url) as response:
            total_size = response.headers.get('content-length')
            if total_size:
                total_size = int(total_size)
                logger.info(f"File size: {total_size / (1024**2):.2f} MB")

            with open(dest_path, 'wb') as out_file:
                chunk_size = 8192
                downloaded = 0
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        progress = (downloaded / total_size) * 100
                        logger.debug(f"Progress: {progress:.1f}%")

        logger.info(f"Successfully downloaded to: {dest_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return False


def extract_zip(zip_path, source_dir):
    """Extract all files from zip directly to source directory, keeping original names."""
    try:
        logger.info(f"Extracting: {zip_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            files_extracted = 0

            # List all files in the zip
            all_files = zip_ref.namelist()
            logger.info(f"Files in zip: {len(all_files)} total")
            for i, file in enumerate(all_files[:10]):  # Show first 10 files
                logger.info(f"  [{i}] {file}")
            if len(all_files) > 10:
                logger.info(f"  ... and {len(all_files) - 10} more")

            # Extract all files (not just CSV)
            for file_info in zip_ref.filelist:
                # Skip directories
                if file_info.filename.endswith('/'):
                    continue

                logger.info(f"Extracting file: {file_info.filename}")
                original_name = Path(file_info.filename).name
                if not original_name:
                    continue

                # Read and write to avoid intermediate folders
                file_content = zip_ref.read(file_info.filename)
                dest_path = source_dir / original_name
                dest_path.write_bytes(file_content)
                files_extracted += 1
                logger.info(f"  Extracted: {original_name} ({len(file_content)} bytes)")

            if files_extracted == 0:
                logger.warning(f"No files found in {zip_path}")
                return False

            logger.info(f"Successfully extracted {files_extracted} files from {zip_path}")
            return True
    except Exception as e:
        logger.error(f"Error extracting {zip_path}: {e}", exc_info=True)
        return False


def download_and_extract(url, source_dir):
    """Download and extract a single dataset."""
    try:
        # Extract year from URL
        year = url.split('_')[-1].split('.')[0]
        zip_filename = f"eCO2mix_RTE_Annuel-Definitif_{year}.zip"
        zip_path = source_dir / zip_filename

        # Check if files for this year already exist (year already present in file names)
        existing_files = [
            file for file in source_dir.glob(f"*{year}*")
            if file.is_file() and file.suffix.lower() != ".zip"
        ]
        if existing_files:
            logger.info(f"Data for year {year} already exists ({len(existing_files)} files), skipping")
            return {"year": year, "success": True, "reason": "already_exists"}

        # Download the file
        if not download_file(url, zip_path):
            return {"year": year, "success": False, "reason": "download_failed"}

        # Extract the file
        if extract_zip(zip_path, source_dir):
            # Remove zip file after extraction
            try:
                zip_path.unlink()
                logger.info(f"Removed zip file: {zip_filename}")
            except Exception as e:
                logger.warning(f"Could not remove zip file {zip_filename}: {e}")

            return {"year": year, "success": True, "reason": "extracted"}
        else:
            return {"year": year, "success": False, "reason": "extraction_failed"}

    except Exception as e:
        logger.error(f"Unexpected error processing {url}: {e}")
        return {"year": "unknown", "success": False, "reason": str(e)}


def main():
    """Download and extract all RTE ECO2MIX datasets in parallel."""
    source_dir = create_source_directory()

    results = []
    successful_downloads = 0
    failed_downloads = 0

    # Use ThreadPoolExecutor for parallel downloads
    max_workers = min(4, len(URLS))  # Limit to 4 concurrent downloads
    logger.info(f"Starting parallel downloads with {max_workers} workers")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_url = {
            executor.submit(download_and_extract, url, source_dir): url
            for url in URLS
        }

        # Process completed tasks as they finish
        for future in as_completed(future_to_url):
            try:
                result = future.result()
                results.append(result)

                if result["success"]:
                    successful_downloads += 1
                    logger.info(f"✓ Year {result['year']}: {result['reason']}")
                else:
                    failed_downloads += 1
                    logger.error(f"✗ Year {result['year']}: {result['reason']}")
            except Exception as e:
                failed_downloads += 1
                logger.error(f"Unexpected error: {e}")

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("Download Summary:")
    logger.info(f"  Successful: {successful_downloads}")
    logger.info(f"  Failed: {failed_downloads}")
    logger.info(f"  Total URLs: {len(URLS)}")
    logger.info(f"  Output directory: {source_dir}")

    # List all extracted files
    all_files = list(source_dir.glob("*"))
    data_files = [f for f in all_files if f.is_file()]  # Exclude directories
    logger.info(f"  Total extracted files: {len(data_files)}")
    logger.info(f"{'='*50}")

    return 0 if failed_downloads == 0 else 1


if __name__ == "__main__":
    exit(main())
