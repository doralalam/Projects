import os
import re
import pandas as pd
import logging
from pathlib import Path

BASE_DIR = Path("/Users/dorababulalam/GitHub/Projects/mf-intelligence")

INPUT_PATH = BASE_DIR / "data/separated_files/edelweiss"
MASTER_BASE = BASE_DIR / "data/master_dataset"

PARQUET_PATH = MASTER_BASE / "parquet_files"
XLSX_PATH = MASTER_BASE / "xlsx_files"

MASTER_PARQUET = PARQUET_PATH / "edelweiss_amc.parquet"
MASTER_XLSX = XLSX_PATH / "edelweiss_amc.xlsx"

LOG_PATH = BASE_DIR / "logs/standardizer_logs"

files_processed = 0
rows_added = 0
errors = 0
data_collector = []

def setup_logging():
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH / "edelweiss_standardizer.log"),
            logging.StreamHandler()
        ]
    )

def detect_header(file_path):
    raw = pd.read_excel(file_path, header=None, nrows=40)
    for i, row in raw.iterrows():
        if row.astype(str).str.contains("ISIN", case=False).any():
            return i
    return 0

def clean_fund_name(fund_name):
    fund_name = fund_name.replace("_", " ")
    fund_name = fund_name.strip()
    fund_name = " ".join(fund_name.split())
    fund_name = fund_name.title()
    abbreviations = ["ELSS", "ESG", "PSU", "BFSI", "MNC", "ETF", "BSE", "IT"]
    for abbr in abbreviations:
        fund_name = re.sub(rf"\b{abbr}\b", abbr, fund_name, flags=re.IGNORECASE)
    if not fund_name.startswith("Edelweiss"):
        fund_name = "Edelweiss " + fund_name
    if not fund_name.endswith("Fund"):
        fund_name = fund_name + " Fund"
    return fund_name

def clean_stock_name(name):
    name = re.sub(r"[^A-Za-z0-9\s]", "", name)
    name = " ".join(name.split())
    return name

def process_file(file_path):
    global rows_added
    header_row = detect_header(file_path)
    df = pd.read_excel(file_path, header=header_row)
    if df.empty:
        return

    stock_col = "Name of the Instrument"
    isin_col = "ISIN"
    sector_col = "Rating/Industry"
    quantity_col = "Quantity"
    market_col = "Market/Fair Value(Rs. In Lacs)"
    weight_col = "% to Net Assets"
    yield_col = "YIELD"

    required = [stock_col, isin_col, sector_col]
    for col in required:
        if col not in df.columns:
            logging.warning(f"Missing column {col} -> {file_path}")
            return

    df[isin_col] = df[isin_col].astype(str).str.strip()
    df[sector_col] = df[sector_col].astype(str).str.strip()

    mask_isin = df[isin_col].str.startswith("INE", na=False)
    mask_yield_blank = df[yield_col].isna() | (df[yield_col].astype(str).str.strip() == "")
    mask_sector_not_nan = df[sector_col].notna() & (df[sector_col].str.lower() != "nan")
    mask_sector_rating = ~df[sector_col].str.startswith(("FITCH", "CRISIL"), na=False)

    df = df[mask_isin & mask_yield_blank & mask_sector_not_nan & mask_sector_rating]
    if df.empty:
        return

    df[quantity_col] = pd.to_numeric(df[quantity_col], errors="coerce")
    df[market_col] = pd.to_numeric(df[market_col], errors="coerce")
    df[weight_col] = pd.to_numeric(df[weight_col], errors="coerce")

    fund_full = file_path.stem
    fund_raw = fund_full.rsplit("_", 2)[0]
    fund = clean_fund_name(fund_raw)

    year = file_path.parents[1].name
    month = file_path.parents[0].name[:3]

    out = pd.DataFrame({
        "amc": "Edelweiss",
        "fund": fund,
        "stock": df[stock_col].astype(str).str.strip().apply(clean_stock_name),
        "isin": df[isin_col],
        "sector": df[sector_col],
        "quantity": df[quantity_col],
        "market_value": df[market_col],
        "weight": df[weight_col],
        "month": month,
        "year": year
    })

    out = out[
        ["amc", "fund", "stock", "isin",
         "sector", "quantity", "market_value",
         "weight", "month", "year"]
    ]

    data_collector.append(out)
    rows_added += len(out)

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

    PARQUET_PATH.mkdir(parents=True, exist_ok=True)
    XLSX_PATH.mkdir(parents=True, exist_ok=True)

    combined.to_parquet(MASTER_PARQUET, index=False)
    combined.to_excel(MASTER_XLSX, index=False)

    logging.info("Parquet dataset overwritten")
    logging.info("Excel dataset overwritten")

def main():
    setup_logging()
    process_all_files()
    update_master_dataset()
    print("\nExecution Summary")
    print("------------------")
    print(f"Files processed: {files_processed}")
    print(f"Rows added: {rows_added}")
    print(f"Errors: {errors}")

if __name__ == "__main__":
    main()