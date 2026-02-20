import os
import pandas as pd
import re

# Paths
DATA_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/sbi"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/sbi"

os.makedirs(OUTPUT_PATH, exist_ok=True)

# Fund mapping
FUND_MAP = {
    "SLMF": "SBI Large and Midcap Fund",
    "SLTEF": "SBI ELSS Tax Saver Fund",
    "SMGLF": "SBI MNC Fund",
    "SCOF": "SBI Consumption Opportunities Fund",
    "STOF": "SBI Technology Opportunities Fund",
    "SHOF": "SBI Healthcare Opportunities Fund",
    "SCF": "SBI Contra Fund",
    "SMCBF-SP": "SBI Children's Fund - Savings Plan",
    "SFEF": "SBI Focused Fund",
    "SCHF": "SBI Conservative Hybrid Fund",
    "SMIDCAP": "SBI Midcap Fund",
    "SMCOMMA": "SBI Comma Fund",
    "SFLEXI": "SBI Flexicap Fund",
    "SMAAF": "SBI Multi Asset Allocation Fund",
    "SBLUECHIP": "SBI Large Cap Fund",
    "SAOF": "SBI Arbitrage Opportunities Fund",
    "SIF": "SBI Infrastructure Fund",
    "SPSU": "SBI PSU Fund",
    "SSCF": "SBI Smallcap Fund",
    "SBPF": "SBI Banking & PSU Fund",
    "SBFS": "SBI Banking And Financial Services Fund",
    "SESF": "SBI Equity Savings Fund",
    "SEMVF": "SBI Equity Minimum Variance Fund",
    "SMCBF-IP": "SBI Children's Fund - Investment Plan",
    "SRBF-AP": "SBI RETIREMENT BENEFIT FUND - AGGRESSIVE PLAN",
    "SRBF-AHP": "SBI RETIREMENT BENEFIT FUND - AGGRESSIVE HYBRID PLAN",
    "SRBF-CHP": "SBI RETIREMENT BENEFIT FUND - CONSERVATIVE HYBRID PLAN",
    "SRBF-CP": "SBI RETIREMENT BENEFIT FUND - CONSERVATIVE PLAN",
    "SBAF": "SBI Balanced Advantage Fund",
    "SMCF": "SBI MultiCap Fund",
    "SDYF": "SBI Dividend Yield Fund",
    "SEOF": "SBI Energy Opportunities Fund",
    "SBI-AOF": "SBI Automotive Opportunities Fund",
    "SIOF": "SBI Innovative Opportunities Fund",
    "SQF": "SBI Quant Fund",
    "SBI Nifty200 Quality 30": "SBI Nifty200 Quality 30 Index Fund",
    "SBI Nifty200 Mom 30": "SBI Nifty200 Momentum 30 Index Fund",
    "SBI Nifty100 Low Vol 30": "SBI Nifty100 Low Volatility 30 Index Fund",
    "SBIRIOS": "SBI Resurgent India Opportunities Scheme"
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
            "Name of the Instrument / Issuer",
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

    if "Name of the Instrument / Issuer" not in df.columns:
        return df

    df = df[df["Name of the Instrument / Issuer"].notna()]

    if "ISIN" in df.columns:
        df = df[df["ISIN"].notna()]

    # Remove section/category headers
    df = df[
        ~df["Name of the Instrument / Issuer"]
        .str.contains(
            "Equity|Debt|Mutual Fund|TREPS|Cash|Derivatives|Total",
            case=False,
            na=False
        )
    ]


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

print("\nSBI separation completed.")
