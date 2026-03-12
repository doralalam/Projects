import os
import pandas as pd
import logging
from pathlib import Path

BASE_DIR = Path("/Users/dorababulalam/GitHub/Projects/mf-intelligence")
INPUT_PATH = BASE_DIR / "data" / "separated_files" / "icici"
MASTER_BASE = BASE_DIR / "data" / "master_dataset"
PARQUET_PATH = MASTER_BASE / "parquet_files"
XLSX_PATH = MASTER_BASE / "xlsx_files"
MASTER_PARQUET = PARQUET_PATH / "icici_amc.parquet"
MASTER_XLSX = XLSX_PATH / "icici_amc.xlsx"
LOG_PATH = BASE_DIR / "logs" / "standardizer_logs"

files_processed = 0
rows_written = 0
errors = 0
data_collector = []

def setup_logging():
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    log_file = LOG_PATH / "icici_standardizer.log"
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
        if row_str.str.contains("isin").any() and row_str.str.contains("coupon").any():
            return i
    return None

def _pick_yield_col(columns):
    lowered = {str(c).strip().lower(): c for c in columns}
    for key in lowered:
        if key == "yield of the instrument":
            return lowered[key]
    for key in lowered:
        if "yield of the instrument" in key:
            return lowered[key]
    for key in lowered:
        if key.startswith("yield"):
            return lowered[key]
    return None

def process_file(file_path):
    global rows_written
    file_path = Path(file_path)
    header_row = detect_header(file_path)
    if header_row is None:
        logging.warning(f"Header not found -> {file_path}")
        return
    df = pd.read_excel(file_path, header=header_row)
    if df.empty:
        return
    unnamed_cols = [c for c in df.columns if str(c).strip().lower().startswith("unnamed:")]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
    df.columns = [str(c).strip() for c in df.columns]
    cols = {str(c).strip().lower(): c for c in df.columns}

    isin_col = cols.get("isin")
    coupon_col = cols.get("coupon")
    stock_col = None
    sector_col = None
    quantity_col = None
    market_value_col = None
    weight_col = None

    for k, v in cols.items():
        if "name of the instrument" in k or "company" in k or "issuer" in k:
            stock_col = v
        elif "industry" in k or "sector" in k or "rating" in k:
            sector_col = v
        elif "quantity" in k:
            quantity_col = v
        elif "market value" in k or "exposure" in k:
            market_value_col = v
        elif "% to nav" in k or "weight" in k:
            weight_col = v

    yield_col = _pick_yield_col(df.columns)

    missing = [name for name, col in [
        ("ISIN", isin_col),
        ("Coupon", coupon_col),
        ("Stock", stock_col),
        ("Sector", sector_col),
        ("Quantity", quantity_col),
        ("Market Value", market_value_col),
        ("Weight", weight_col),
        ("Yield", yield_col)
    ] if col is None]

    if missing:
        logging.warning(f"Missing required columns {missing} -> {file_path}")
        return

    isin = df[isin_col].astype(str).str.strip()
    mask_isin = isin.str.startswith("INE", na=False)
    coupon_blank = df[coupon_col].map(_is_blank)
    yield_blank = df[yield_col].map(_is_blank)
    df_filtered = df.loc[mask_isin & coupon_blank & yield_blank].copy()
    if df_filtered.empty:
        return

    for col in [quantity_col, market_value_col, weight_col]:
        df_filtered[col] = pd.to_numeric(df_filtered[col].astype(str).str.replace(",", "").str.strip(), errors="coerce")

    fund = file_path.stem
    try:
        year = file_path.parents[2].name
        month = file_path.parents[1].name
    except Exception:
        year = "0000"
        month = "UNK"

    out = df_filtered.loc[:, [stock_col, isin_col, sector_col, quantity_col, market_value_col, weight_col]].copy()
    out = out.rename(columns={
        stock_col: "stock",
        isin_col: "isin",
        sector_col: "sector",
        quantity_col: "quantity",
        market_value_col: "market_value",
        weight_col: "weight"
    })
    out = out.assign(amc="ICICI", fund=fund, month=month, year=year)
    out = out[["amc", "fund", "stock", "isin", "sector", "quantity", "market_value", "weight", "month", "year"]]

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
    required_subset = ["amc", "fund", "isin", "month", "year"]
    existing_subset = [c for c in required_subset if c in combined.columns]
    combined = combined.drop_duplicates(subset=existing_subset, keep="last")
    combined.to_parquet(MASTER_PARQUET, index=False)
    combined.to_excel(MASTER_XLSX, index=False)
    logging.info(f"Parquet dataset updated -> {MASTER_PARQUET}")
    logging.info(f"Excel dataset updated -> {MASTER_XLSX}")

def main():
    setup_logging()
    logging.info("Starting ICICI standardization")
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