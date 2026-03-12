import os
import pandas as pd
import logging
import re

RAW_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/motilal"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/motilal"
LOG_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/logs/separator_logs"

FUND_MAP = {
    "YO20": "Motilal Oswal Large and Midcap Fund",
    "YO07": "Motilal Oswal Midcap Fund",
    "YO46": "Motilal Oswal Small Cap Fund",
    "YO05": "Motilal Oswal Focused Fund",
    "YO08": "Motilal Oswal Flexi Cap Fund",
    "YO47": "Motilal Oswal Large Cap Fund",
    "YO09": "Motilal Oswal ELSS Tax Saver Fund",
    "YO10": "Motilal Oswal Balanced Advantage Fund",
    "YO50": "Motilal Oswal Quant Fund",
    "YO51": "Motilal Oswal Multicap Fund",
    "YO53": "Motilal Oswal Manufacturing Fund",
    "Y054": "Motilal Oswal Business Cycle Fund",
    "YO58": "Motilal Oswal Digital India Fund",
    "YO65": "Motilal Oswal Innovation Opportunities Fund",
    "YO66": "Motilal Oswal Active Momentum Fund",
    "YO68": "Motilal Oswal Infrastructure Fund",
    "YO72": "Motilal Oswal Services Fund",
    "YO80": "Motilal Oswal Special Opportunities Fund",
    "YO82": "Motilal Oswal Consumption Fund"
}

files_processed = 0
sheets_extracted = 0
error_count = 0


def setup_logging():

    os.makedirs(LOG_PATH, exist_ok=True)

    log_file = os.path.join(LOG_PATH, "motilal_separator.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def extract_date_parts(file_name):

    match = re.search(
        r'(jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|september|oct|october|nov|november|dec|december)[-_ ]*(\d{2,4})',
        file_name,
        re.IGNORECASE
    )

    if not match:
        raise ValueError(f"DATE PARSE FAILED -> {file_name}")

    month_raw = match.group(1).lower()
    year = match.group(2)

    if len(year) == 2:
        year = "20" + year

    month_map = {
        "jan":"Jan","january":"Jan",
        "feb":"Feb","february":"Feb",
        "mar":"Mar","march":"Mar",
        "apr":"Apr","april":"Apr",
        "may":"May",
        "jun":"Jun","june":"Jun",
        "jul":"Jul","july":"Jul",
        "aug":"Aug","august":"Aug",
        "sep":"Sep","september":"Sep",
        "oct":"Oct","october":"Oct",
        "nov":"Nov","november":"Nov",
        "dec":"Dec","december":"Dec"
    }

    return month_map[month_raw], year


def split_sheets(file_path, month, year):

    global sheets_extracted

    xls = pd.ExcelFile(file_path)

    for sheet in xls.sheet_names:

        if sheet not in FUND_MAP:
            continue

        fund_name = FUND_MAP[sheet]

        logging.info(f"Splitting sheet {sheet} -> {fund_name}")

        df = pd.read_excel(file_path, sheet_name=sheet, header=None)

        output_dir = os.path.join(OUTPUT_PATH, year, month)

        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(
            output_dir,
            f"{fund_name}_{month}_{year}.xlsx"
        )

        df.to_excel(output_file, index=False, header=False)

        sheets_extracted += 1

        logging.info(f"Saved -> {output_file}")


def process_files():

    global files_processed, error_count

    for root, _, files in os.walk(RAW_PATH):

        for file in files:

            if not file.endswith((".xls", ".xlsx")):
                continue

            file_path = os.path.join(root, file)

            try:
                month, year = extract_date_parts(file)

            except Exception as e:

                logging.error(str(e))

                error_count += 1

                continue

            logging.info(f"Processing -> {file} ({month}-{year})")

            files_processed += 1

            try:

                split_sheets(file_path, month, year)

            except Exception as e:

                logging.error(f"Processing failed -> {file}")

                logging.error(str(e))

                error_count += 1


def main():

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    setup_logging()

    logging.info("Starting Motilal sheet splitting")

    process_files()

    print("\nExecution Summary")
    print("------------------")
    print(f"Files processed: {files_processed}")
    print(f"Sheets extracted: {sheets_extracted}")
    print(f"Errors: {error_count}")

    if error_count > 0:

        logging.warning("Task partially completed. Please resolve errors.")

        print("\nTask partially completed. Check logs.")

    else:

        logging.info("Task completed successfully.")

        print("\nTask completed successfully.")


if __name__ == "__main__":
    main()