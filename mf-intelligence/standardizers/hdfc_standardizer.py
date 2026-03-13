import os
import re
import pandas as pd
import logging
from pathlib import Path

BASE_DIR = Path("/Users/dorababulalam/GitHub/Projects/mf-intelligence")
INPUT_PATH = BASE_DIR / "data" / "separated_files" / "hdfc"
MASTER_BASE = BASE_DIR / "data" / "master_dataset"
PARQUET_PATH = MASTER_BASE / "parquet_files"
XLSX_PATH = MASTER_BASE / "xlsx_files"
MASTER_PARQUET = PARQUET_PATH / "hdfc_amc.parquet"
MASTER_XLSX = XLSX_PATH / "hdfc_amc.xlsx"
LOG_PATH = BASE_DIR / "logs" / "standardizer_logs"

files_processed = 0
rows_written = 0
errors = 0
data_collector = []

def setup_logging():
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    log_file = LOG_PATH / "hdfc_standardizer.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )

def _is_blank(value) -> bool:
    if value is None or pd.isna(value):
        return True
    if isinstance(value, str):
        s = value.strip()
        return s == "" or s.lower() == "nan"
    return False

def detect_header(file_path: Path):
    raw = pd.read_excel(file_path, header=None, nrows=80)
    for i, row in raw.iterrows():
        row_str = row.astype(str).str.lower()
        if row_str.str.contains("isin").any():
            return i
    return 0

def find_column(df, keywords):
    for k, v in enumerate(df.columns):
        for kw in keywords:
            if kw.lower() in str(v).strip().lower():
                return v
    return None

def process_file(file_path):
    global rows_written
    file_path = Path(file_path)
    header_row = detect_header(file_path)
    df = pd.read_excel(file_path, header=header_row)
    if df.empty:
        return
    unnamed_cols = [c for c in df.columns if str(c).strip().lower().startswith("unnamed:")]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
    isin_col = find_column(df, ["isin"])
    stock_col = find_column(df, ["name of the instrument", "company", "issuer"])
    sector_col = find_column(df, ["industry", "sector", "rating"])
    quantity_col = find_column(df, ["quantity"])
    market_value_col = find_column(df, ["market", "fair value", "exposure"])
    weight_col = find_column(df, ["% to nav", "weight"])
    coupon_col = find_column(df, ["coupon"])
    yield_col = find_column(df, ["yield"])
    missing = [name for name, col in [
        ("ISIN", isin_col),
        ("Stock", stock_col),
        ("Sector", sector_col),
        ("Quantity", quantity_col),
        ("Market Value", market_value_col),
        ("Weight", weight_col),
        ("Coupon", coupon_col),
        ("Yield", yield_col)
    ] if col is None]
    if missing:
        logging.warning(f"Missing required columns {missing} -> {file_path}")
        return
    df[isin_col] = df[isin_col].astype(str).str.strip()
    mask_not_blank = ~df[isin_col].apply(_is_blank)
    mask_starts_ine = df[isin_col].str.startswith("INE", na=False)
    df[coupon_col] = df[coupon_col].astype(str)
    df[yield_col] = df[yield_col].astype(str)
    mask_coupon_blank = df[coupon_col].apply(_is_blank)
    mask_yield_blank = df[yield_col].apply(_is_blank)
    df_filtered = df.loc[mask_not_blank & mask_starts_ine & mask_coupon_blank & mask_yield_blank].copy()
    if df_filtered.empty:
        return
    for col in [quantity_col, market_value_col, weight_col]:
        df_filtered[col] = pd.to_numeric(
            df_filtered[col].astype(str).str.replace(",", "").str.strip(),
            errors="coerce"
        )
    fund_full = file_path.stem
    fund = re.sub(r"^Monthly\s+", "", fund_full)
    fund = re.sub(r"\s+-\s+\d{1,2}\s\w+\s\d{4}$", "", fund)
    year = file_path.parents[1].name
    month = file_path.parents[0].name[:3]
    out = df_filtered.loc[:, [stock_col, isin_col, sector_col, quantity_col, market_value_col, weight_col]].copy()
    out = out.rename(columns={
        stock_col: "stock",
        isin_col: "isin",
        sector_col: "sector",
        quantity_col: "quantity",
        market_value_col: "market_value",
        weight_col: "weight"
    })
    out = out.assign(amc="HDFC", fund=fund, month=month, year=year)
    out = out[["amc", "fund", "stock", "isin", "sector", "quantity", "market_value", "weight", "month", "year"]]
    for col in ["stock", "isin", "sector", "fund", "amc", "month", "year"]:
        out[col] = out[col].astype(str).str.strip()
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
    new_data = pd.concat(data_collector, ignore_index=True)
    os.makedirs(PARQUET_PATH, exist_ok=True)
    os.makedirs(XLSX_PATH, exist_ok=True)
    if MASTER_PARQUET.exists():
        existing = pd.read_parquet(MASTER_PARQUET)
        combined = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined = new_data
    subset_cols = ["amc", "fund", "isin", "month", "year"]
    combined = combined.drop_duplicates(subset=subset_cols, keep="last")
    combined.to_parquet(MASTER_PARQUET, index=False)
    combined.to_excel(MASTER_XLSX, index=False)
    logging.info(f"Parquet dataset updated -> {MASTER_PARQUET}")
    logging.info(f"Excel dataset updated -> {MASTER_XLSX}")

def main():
    setup_logging()
    logging.info("Starting HDFC standardization")
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