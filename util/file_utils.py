import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

def get_validated_file(importer, filename):
    logging.info("Validating file...")
    base_path = importer.source_dataset_path or "data/"
    base_path = Path(base_path)

    if not base_path.is_dir():
        print(f"Error: {base_path} isn't a valid directory.")
        sys.exit(1)

    file_path = base_path / filename
    if not file_path.is_file():
        print(f"Error: {file_path} doesn't exist in {base_path}")
        sys.exit(1)

    return file_path