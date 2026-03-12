import os
import pandas as pd
import logging

RAW_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/nippon_scraped"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/nippon"
LOG_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/logs/separator_logs"

FUND_MAP = {
    "GF": "Nippon India Growth Mid Cap Fund",
    "GS": "Nippon India Vision Large & Mid Cap Fund",
    "BF": "Nippon India Banking and Financial Services Fund",
    "PS": "Nippon India Power & Infra Fund",
    "PH": "Nippon India Pharma Fund",
    "ME": "Nippon India Consumption Fund",
    "NE": "Nippon India Balanced Advantage Fund",
    "EO": "Nippon India Multi Cap Fund",
    "SE": "Nippon India Value Fund",
    "SH": "Nippon India Aggressive Hybrid Fund",
    "TS": "Nippon India ELSS Tax Saver Fund",
    "LE": "Nippon India Focused Fund",
    "EA": "Nippon India Large Cap Fund",
    "QP": "Nippon India Quant Fund",
    "SC": "Nippon India Small Cap Fund",
    "AF": "Nippon India Arbitrage Fund",
    "MF": "Nippon India Multi Asset Allocation Fund",
    "LC": "Nippon India Flexi Cap Fund",
    "IT": "Nippon India Innovation Fund",
    "AM": "Nippon India Active Momentum Fund",
    "QI": "Nippon India Nifty 500 Quality 50 Index Fund",
    "MC": "Nippon India MNC Fund"
}


def setup_logging():
    os.makedirs(LOG_PATH, exist_ok=True)
    log_file = os.path.join(LOG_PATH, "nippon_separator.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def split_sheets(file_path, year, month):

    xls = pd.ExcelFile(file_path)

    for sheet in xls.sheet_names:

        if sheet not in FUND_MAP:
            continue

        fund_name = FUND_MAP[sheet]

        logging.info(f"Splitting sheet: {sheet} -> {fund_name}")

        df = pd.read_excel(file_path, sheet_name=sheet, header=None)

        output_dir = os.path.join(OUTPUT_PATH, year, month)
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(
            output_dir,
            f"{fund_name}_{month}_{year}.xlsx"
        )

        df.to_excel(output_file, index=False, header=False)

        logging.info(f"Saved file: {output_file}")


def process_files():

    for root, _, files in os.walk(RAW_PATH):

        for file in files:

            if not file.endswith((".xls", ".xlsx")):
                continue

            file_path = os.path.join(root, file)

            parts = root.split(os.sep)

            year = parts[-2]
            month = parts[-1]

            logging.info(f"Processing file: {file} ({month}-{year})")

            try:
                split_sheets(file_path, year, month)
            except Exception as e:
                logging.error(f"Error processing {file}: {e}")


def main():

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    setup_logging()

    logging.info("Starting Nippon sheet splitting...")

    process_files()

    logging.info("Nippon sheet separation completed.")


if __name__ == "__main__":
    main()