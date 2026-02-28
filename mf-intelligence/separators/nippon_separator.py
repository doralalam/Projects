import os
import pandas as pd
import re


# Paths
DATA_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/nippon"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/nippon"

os.makedirs(OUTPUT_PATH, exist_ok=True)


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


# ---------------------------------------------------
# DATE EXTRACTION (Robust)
# ---------------------------------------------------
def extract_date_parts(file_name):

    match = re.search(
        r'(Jan|Feb|Mar|Apr|April|May|Jun|June|Jul|July|Aug|Sep|Oct|Nov|November|Dec)[-\s](\d{2})',
        file_name,
        re.IGNORECASE
    )

    if not match:
        return "UNK", "0000"

    month_raw = match.group(1)
    year_short = match.group(2)

    month_map = {
        "Jan": "Jan", "Feb": "Feb", "Mar": "Mar",
        "Apr": "Apr", "April": "Apr",
        "May": "May",
        "Jun": "Jun", "June": "Jun",
        "Jul": "Jul", "July": "Jul",
        "Aug": "Aug", "Sep": "Sep",
        "Oct": "Oct",
        "Nov": "Nov", "November": "Nov",
        "Dec": "Dec",
    }

    month = month_map.get(month_raw, "UNK")
    year = "20" + year_short

    return month, year


# ---------------------------------------------------
# Robust Excel Loader
# ---------------------------------------------------
def load_excel_file(file_path):
    try:
        return pd.ExcelFile(file_path)
    except Exception:
        return pd.ExcelFile(file_path, engine="openpyxl")


def read_excel_safe(file_path, sheet_name, header_row):
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
    except Exception:
        return pd.read_excel(file_path, sheet_name=sheet_name, header=header_row, engine="openpyxl")


# ---------------------------------------------------
# HEADER DETECTION (Improved)
# ---------------------------------------------------
def find_header_row(file_path, sheet_name):

    try:
        temp_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    except Exception:
        temp_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine="openpyxl")

    for i, row in temp_df.iterrows():
        row_str = row.astype(str).str.lower()

        if (
            any("isin" in val for val in row_str) and
            any("name of the instrument" in val for val in row_str)
        ):
            return i

    return None


# ---------------------------------------------------
# CLEAN PORTFOLIO DATA
# ---------------------------------------------------
def clean_portfolio_dataframe(df):

    df = df.dropna(how="all")
    df.columns = df.columns.str.strip()

    # Normalize column names
    rename_map = {}

    for col in df.columns:
        col_lower = col.lower()

        if "name of the instrument" in col_lower:
            rename_map[col] = "Name of the Instrument"

        elif col_lower.strip() == "isin":
            rename_map[col] = "ISIN"

        elif "% to nav" in col_lower:
            rename_map[col] = "% to NAV"

        elif "industry" in col_lower:
            rename_map[col] = "Industry"

    df = df.rename(columns=rename_map)

    if "Name of the Instrument" not in df.columns:
        return df

    # Remove blank rows
    df = df[df["Name of the Instrument"].notna()]

    # Keep only valid ISIN rows (equity filter)
    if "ISIN" in df.columns:
        df = df[
            df["ISIN"]
            .astype(str)
            .str.match(r'^INE[A-Z0-9]{9}$', na=False)
        ]

    # Remove category rows
    df = df[
        ~df["Name of the Instrument"]
        .str.contains(
            "Equity|Debt|Mutual Fund|TREPS|Cash|Derivatives|Total",
            case=False,
            na=False
        )
    ]

    return df.reset_index(drop=True)


# ---------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------
for file in os.listdir(DATA_PATH):

    if not (file.endswith(".xls") or file.endswith(".xlsx")):
        continue

    file_path = os.path.join(DATA_PATH, file)
    month, year = extract_date_parts(file)

    print(f"\nProcessing -> {file} ({month}-{year})")

    try:

        xls = load_excel_file(file_path)

        for sheet in xls.sheet_names:

            if sheet not in FUND_MAP:
                continue

            fund_name = FUND_MAP[sheet]

            fund_folder = os.path.join(OUTPUT_PATH, fund_name)
            os.makedirs(fund_folder, exist_ok=True)

            print(f"   -> Extracting {sheet} -> {fund_name}")

            header_row = find_header_row(file_path, sheet)

            if header_row is None:
                print(f"      Header not found -> {sheet}")
                continue

            df = read_excel_safe(file_path, sheet, header_row)

            df = clean_portfolio_dataframe(df)

            output_file = os.path.join(
                fund_folder,
                f"{fund_name}_{month}_{year}.xlsx"
            )

            df.to_excel(output_file, index=False)

    except Exception as e:
        print(f"Failed -> {file} | {e}")

print("\nNippon separation completed successfully.")