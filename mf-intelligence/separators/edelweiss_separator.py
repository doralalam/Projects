import os
import pandas as pd
import logging
import re

RAW_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/edelweiss"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/edelweiss"
LOG_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/logs/separator_logs"

FUND_MAP = {
    "EEECRF": "Edelweiss_Flexi_Cap_Fund",
    "EECONF": "Edelweiss_Consumption_Fund",
    "EEESSF": "Edelweiss_Equity_Savings_Fund",
    "EEEQTF": "Edelweiss_Large_and_Mid_Cap_Fund",
    "EEMAAF": "Edelweiss_Multi_Asset_Allocation_Fund",
    "EEARFD": "Edelweiss_Balanced_Advantage_Fund",
    "EEELSS": "Edelweiss_ELSS_Tax_Saver_Fund",
    "EEFOCF": "Edelweiss_Focused_Fund",
    "EEBCYF": "Edelweiss_Business_Cycle_Fund",
    "EEDGEF": "Edelweiss_Large_Cap_Fund",
    "EEARBF": "Edelweiss_Arbitrage_Fund",
    "EEPRUA": "Edelweiss_Aggressive_Hybrid_Fund",
    "EEESCF": "Edelweiss_Small_Cap_Fund",
    "EEEMCF": "Edelweiss_Mid_Cap_Fund",
    "EETECF": "Edelweiss_Technology_Fund"
}

files_processed = 0
sheets_extracted = 0
error_count = 0


def setup_logging():

    os.makedirs(LOG_PATH, exist_ok=True)

    log_file = os.path.join(LOG_PATH, "edelweiss_separator.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def extract_date_parts(file_name):

    match = re.search(r"\d{2}-([A-Za-z]{3})-(\d{4})", file_name)

    if not match:
        raise ValueError(f"DATE PARSE FAILED -> {file_name}")

    month, year = match.groups()

    return month, year


def find_header_row(file_path, sheet_name):

    temp_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    for i, row in temp_df.iterrows():

        if row.astype(str).str.contains(
            "Name of the Instrument",
            case=False,
            na=False
        ).any():
            return i

    return None


def clean_portfolio_dataframe(df):

    df = df.dropna(how="all")

    if "Name of the Instrument" not in df.columns:
        return df

    df = df[df["Name of the Instrument"].notna()]

    df = df[
        ~df["Name of the Instrument"]
        .str.contains(
            "Equity|Debt|Mutual Fund|TREPS|Cash|Derivatives|Total",
            case=False,
            na=False
        )
    ]

    if "% to Net Assets" in df.columns:

        col = "% to Net Assets"

        df[col] = (
            df[col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
        )

        df[col] = pd.to_numeric(df[col], errors="coerce")

        df[col] = df[col].apply(
            lambda x: x * 100 if pd.notna(x) and x <= 1 else x
        )

        df[col] = df[col].round(4)

    return df.reset_index(drop=True)


def process_files():

    global files_processed, sheets_extracted, error_count

    for root, _, files in os.walk(RAW_PATH):

        for file in files:

            if not file.endswith(".xlsx"):
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

                xls = pd.ExcelFile(file_path)

                for sheet in xls.sheet_names:

                    if sheet not in FUND_MAP:
                        continue

                    fund_name = FUND_MAP[sheet]

                    logging.info(f"Extracting -> {sheet}")

                    header_row = find_header_row(file_path, sheet)

                    if header_row is None:
                        logging.warning(f"Header not found -> {sheet}")
                        continue

                    df = pd.read_excel(
                        file_path,
                        sheet_name=sheet,
                        header=header_row
                    )

                    df = clean_portfolio_dataframe(df)

                    output_dir = os.path.join(OUTPUT_PATH, year, month)

                    os.makedirs(output_dir, exist_ok=True)

                    output_file = os.path.join(
                        output_dir,
                        f"{fund_name}_{month}_{year}.xlsx"
                    )

                    df.to_excel(output_file, index=False)

                    sheets_extracted += 1

                    logging.info(f"Saved -> {output_file}")

            except Exception as e:

                logging.error(f"Processing failed -> {file}")

                logging.error(str(e))

                error_count += 1


def main():

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    setup_logging()

    logging.info("Starting Edelweiss sheet splitting")

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