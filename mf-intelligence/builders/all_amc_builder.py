import os
import pandas as pd
import logging
from pathlib import Path

BASE_DIR = Path("/Users/dorababulalam/GitHub/Projects/mf-intelligence")

PARQUET_DIR = BASE_DIR / "data" / "master_dataset" / "parquet_files"

OUTPUT_DIR = BASE_DIR / "data" / "master_dataset" / "all_amc"

MASTER_PARQUET = OUTPUT_DIR / "all_amc.parquet"
MASTER_XLSX = OUTPUT_DIR / "all_amc.xlsx"

LOG_FILE = BASE_DIR / "logs" / "builders" / "all_amc_builder.log"


def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )


def build_master_dataset():

    datasets = []
    files_processed = 0

    for file in os.listdir(PARQUET_DIR):

        if not file.endswith(".parquet"):
            continue

        file_path = PARQUET_DIR / file

        try:

            logging.info(f"Reading -> {file}")

            df = pd.read_parquet(file_path)

            datasets.append(df)

            files_processed += 1

        except Exception as e:

            logging.error(f"Failed reading {file}")
            logging.error(str(e))

    if not datasets:
        logging.warning("No parquet files found.")
        return

    logging.info("Combining datasets...")

    combined = pd.concat(datasets, ignore_index=True)

    combined = combined.drop_duplicates(
        subset=["amc", "fund", "isin", "month", "year"]
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("Saving master dataset...")

    combined.to_parquet(MASTER_PARQUET, index=False)

    combined.to_excel(MASTER_XLSX, index=False)

    print("\nExecution Summary")
    print("------------------")
    print(f"Files processed : {files_processed}")
    print(f"Total rows      : {len(combined)}")
    print(f"Total funds     : {combined['fund'].nunique()}")
    print(f"Total stocks    : {combined['stock'].nunique()}")
    print(f"Total AMCs      : {combined['amc'].nunique()}")


def main():

    setup_logging()

    build_master_dataset()


if __name__ == "__main__":
    main()