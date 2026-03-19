import os
import re
import pandas as pd
import logging
from pathlib import Path

BASE_DIR = Path("/Users/dorababulalam/GitHub/Projects/mf-intelligence")
INPUT_PATH = BASE_DIR / "data" / "separated_files" / "invesco"

MASTER_BASE = BASE_DIR / "data" / "master_dataset"
PARQUET_PATH = MASTER_BASE / "parquet_files"
XLSX_PATH = MASTER_BASE / "xlsx_files"

MASTER_PARQUET = PARQUET_PATH / "invesco_amc.parquet"
MASTER_XLSX = XLSX_PATH / "invesco_amc.xlsx"

LOG_PATH = BASE_DIR / "logs" / "standardizer_logs"

files_processed = 0
rows_written = 0
errors = 0
data_collector = []


def setup_logging():

    LOG_PATH.mkdir(parents=True, exist_ok=True)

    log_file = LOG_PATH / "invesco_standardizer.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )


def detect_header(file_path: Path):

    raw = pd.read_excel(file_path, header=None)

    for i, row in raw.iterrows():

        row_str = row.astype(str).str.lower()

        if row_str.str.contains("isin").any() and row_str.str.contains("name of the instrument").any():
            return i

    return None


def normalize_columns(df: pd.DataFrame):

    col_map = {}

    for c in df.columns:

        cl = str(c).strip().lower()

        if "name of the instrument" in cl:
            col_map[c] = "stock"

        elif cl == "isin":
            col_map[c] = "isin"

        elif "industry" in cl or "sector" in cl:
            col_map[c] = "sector"

        elif "quantity" in cl:
            col_map[c] = "quantity"

        elif "market/fair value" in cl or "market value" in cl:
            col_map[c] = "market_value"

        elif "% to net assets" in cl or "% to nav" in cl:
            col_map[c] = "weight"

    return df.rename(columns=col_map)


def clean_numeric_columns(df: pd.DataFrame):

    numeric_cols = ["quantity", "market_value", "weight"]

    for col in numeric_cols:

        if col not in df.columns:
            continue

        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip()
        )

        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def clean_fund_name(fund_name):

    fund_name = fund_name.replace("_", " ")
    fund_name = fund_name.strip()

    fund_name = re.sub(r"\s+", " ", fund_name)

    fund_name = fund_name.title()

    abbreviations = ["ELSS", "ESG", "PSU", "BFSI", "MNC", "ETF", "BSE"]

    for abbr in abbreviations:
        fund_name = re.sub(rf"\b{abbr}\b", abbr, fund_name, flags=re.IGNORECASE)

    if not fund_name.startswith("Invesco"):
        fund_name = "Invesco " + fund_name

    if not fund_name.endswith("Fund"):
        fund_name = fund_name + " Fund"

    return fund_name


def extract_metadata(file_name: str):

    name = file_name.replace(".xlsx", "")

    parts = name.split("_")

    if len(parts) < 3:

        fund = "Invesco Unknown Fund"
        month = "UNK"
        year = "0000"

    else:

        year = parts[-1]
        month = parts[-2]

        fund_raw = " ".join(parts[:-3])

        fund = clean_fund_name(fund_raw)

    return fund, month, year


def process_file(file_path):

    global rows_written

    file_path = Path(file_path)

    header_row = detect_header(file_path)

    if header_row is None:

        logging.warning(f"Header not found -> {file_path}")

        return

    df = pd.read_excel(file_path, header=header_row)

    df = normalize_columns(df)

    df = clean_numeric_columns(df)

    if "isin" not in df.columns:

        logging.warning(f"ISIN column missing -> {file_path}")

        return

    df = df[df["isin"].astype(str).str.match(r"^INE[A-Z0-9]{9}$", na=False)]

    if "sector" in df.columns:

        df["sector"] = (
            df["sector"]
            .astype(str)
            .str.strip()
            .replace(["NA", "N.A", "N.A.", "N A", "nan", "None"], "")
        )

        df = df[df["sector"].str.strip() != ""]

    fund, month, year = extract_metadata(file_path.name)

    required_cols = ["stock", "isin", "sector", "quantity", "market_value", "weight"]

    for col in required_cols:

        if col not in df.columns:
            df[col] = None

    out = df[required_cols].copy()

    out.insert(0, "amc", "Invesco")

    out.insert(1, "fund", fund)

    out["month"] = month

    out["year"] = year

    out = out.dropna(subset=["isin"])

    data_collector.append(out)

    rows_written += len(out)


def process_all_files():

    global files_processed, errors

    for root, _, files in os.walk(INPUT_PATH):

        for file in files:

            if not file.endswith(".xlsx"):
                continue

            file_path = Path(root) / file

            try:

                logging.info(f"Processing -> {file}")

                process_file(file_path)

                files_processed += 1

            except Exception as e:

                logging.error(f"Failed -> {file}")

                logging.error(str(e))

                errors += 1


def update_master_dataset():

    if not data_collector:

        logging.info("No data collected. Master dataset not updated.")

        return

    combined = pd.concat(data_collector, ignore_index=True)

    combined = combined.drop_duplicates(
        subset=["amc", "fund", "isin", "month", "year"],
        keep="last"
    )

    os.makedirs(PARQUET_PATH, exist_ok=True)

    os.makedirs(XLSX_PATH, exist_ok=True)

    combined.to_parquet(MASTER_PARQUET, index=False)

    combined.to_excel(MASTER_XLSX, index=False)

    logging.info(f"Parquet dataset overwritten -> {MASTER_PARQUET}")

    logging.info(f"Excel dataset overwritten -> {MASTER_XLSX}")


def main():

    setup_logging()

    logging.info("Starting Invesco standardization")

    process_all_files()

    update_master_dataset()

    print("\nExecution Summary")

    print("------------------")

    print(f"Files processed: {files_processed}")

    print(f"Rows added: {rows_written}")

    print(f"Errors: {errors}")

    if errors > 0:

        logging.warning("Task partially completed. Resolve errors.")

        print("\nTask partially completed. Check logs.")

    else:

        logging.info("Task completed successfully.")

        print("\nTask completed successfully.")


if __name__ == "__main__":

    main()