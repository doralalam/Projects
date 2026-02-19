import os
import pandas as pd
import re

# Paths
DATA_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/edelweiss"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/edelweiss"

os.makedirs(OUTPUT_PATH, exist_ok=True)

# Fund mapping
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


def extract_date_parts(file_name):
    """
    Extract Month (3 chars) + Year
    Example: 31-Jan-2026 → Jan, 2026
    """
    match = re.search(r"\d{2}-([A-Za-z]{3})-(\d{4})", file_name)

    if not match:
        return "UNK", "0000"

    month, year = match.groups()
    return month, year


# find Header
def find_header_row(file_path, sheet_name):
    """
    Detect header row dynamically
    by locating 'Name of the Instrument'
    """

    temp_df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=None
    )

    for i, row in temp_df.iterrows():

        if row.astype(str).str.contains(
            "Name of the Instrument",
            case=False,
            na=False
        ).any():
            return i

    return None



def clean_portfolio_dataframe(df):
    """
    Cleans category rows & fixes % column
    """

    # Drop fully empty rows
    df = df.dropna(how="all")

    if "Name of the Instrument" not in df.columns:
        return df

    df = df[df["Name of the Instrument"].notna()]

    # Remove section/category headers
    df = df[
        ~df["Name of the Instrument"]
        .str.contains(
            "Equity|Debt|Mutual Fund|TREPS|Cash|Derivatives|Total",
            case=False,
            na=False
        )
    ]

# Percentage to float value
    if "% to Net Assets" in df.columns:

        col = "% to Net Assets"

        df[col] = (
            df[col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
        )

        df[col] = pd.to_numeric(df[col], errors="coerce")

        # Row-level scaling
        df[col] = df[col].apply(
            lambda x: x * 100 if pd.notna(x) and x <= 1 else x
        )

        df[col] = df[col].round(4)

    return df.reset_index(drop=True)


## Separate sheets
for file in os.listdir(DATA_PATH):

    if not file.endswith(".xlsx"):
        continue

    file_path = os.path.join(DATA_PATH, file)
    month, year = extract_date_parts(file)

    print(f"\nProcessing -> {file} ({month}-{year})")

    try:
        xls = pd.ExcelFile(file_path)

        for sheet in xls.sheet_names:

            if sheet not in FUND_MAP:
                continue

            fund_name = FUND_MAP[sheet]

            # Create fund folder
            fund_folder = os.path.join(OUTPUT_PATH, fund_name)
            os.makedirs(fund_folder, exist_ok=True)

            print(f"   -> Extracting {sheet} -> {fund_name}")

            header_row = find_header_row(file_path, sheet)

            if header_row is None:
                print(f"      Header not found -> {sheet}")
                continue


            df = pd.read_excel(
                file_path,
                sheet_name=sheet,
                header=header_row
            )

       
            df = clean_portfolio_dataframe(df)


            output_file = os.path.join(
                fund_folder,
                f"{fund_name}_{month}_{year}.xlsx"
            )

            df.to_excel(output_file, index=False)

    except Exception as e:
        print(f"Failed -> {file} | {e}")

print("\nEdelweiss separation completed.")
