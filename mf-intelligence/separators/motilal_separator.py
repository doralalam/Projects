import os
import pandas as pd
import re

# Paths
DATA_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/motilal"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/motilal"

os.makedirs(OUTPUT_PATH, exist_ok=True)

# Fund mapping
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


def extract_date_parts(file_name):
    """
    Extract Month (3-letter format) + Year
    Example:
    b5209-scheme-portfolio-details-january-2026-2-
    → Jan, 2026
    """

    match = re.search(
        r'(january|february|march|april|may|june|july|august|september|october|november|december)-(\d{4})',
        file_name,
        re.IGNORECASE
    )

    if not match:
        return "UNK", "0000"

    month_full = match.group(1).lower()
    year = match.group(2)

    month_map = {
        "january": "Jan",
        "february": "Feb",
        "march": "Mar",
        "april": "Apr",
        "may": "May",
        "june": "Jun",
        "july": "Jul",
        "august": "Aug",
        "september": "Sep",
        "october": "Oct",
        "november": "Nov",
        "december": "Dec"
    }

    month = month_map.get(month_full, "UNK")

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
            "Name of Instrument",
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

    if "Name of Instrument" not in df.columns:
        return df

    df = df[df["Name of Instrument"].notna()]

    if "ISIN" in df.columns:
        df = df[df["ISIN"].notna()]

    # Remove section/category headers
    df = df[
        ~df["Name of Instrument"]
        .str.contains(
            "Equity|Debt|Mutual Fund|TREPS|Cash|Derivatives|Total",
            case=False,
            na=False
        )
    ]

# Percentage to float value
    if "% to Net Assets" in df.columns:

        col = "% to Net Assets"

        df[col] = pd.to_numeric(df[col], errors="coerce")

        df[col] = (df[col] * 100).round(4)


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

print("\nMotilal separation completed.")
