import os
import pandas as pd
import logging
import re

RAW_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/sbi_scraped"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/sbi"
LOG_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/logs/separator_logs"

os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

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

files_processed = 0
sheets_extracted = 0
error_count = 0

def setup_logging():
    log_file = os.path.join(LOG_PATH, "sbi_separator.log")
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
        r'(january|february|march|april|may|june|july|august|september|october|november|december)[-_ ]*(\d{2,4})',
        file_name,
        re.IGNORECASE
    )
    if not match:
        raise ValueError(f"DATE PARSE FAILED -> {file_name}")
    month_full = match.group(1).lower()
    year = match.group(2)
    if len(year) == 2:
        year = "20" + year
    month_map = {
        "january": "Jan","february": "Feb","march": "Mar",
        "april": "Apr","may": "May","june": "Jun",
        "july": "Jul","august": "Aug","september": "Sep",
        "october": "Oct","november": "Nov","december": "Dec"
    }
    return month_map[month_full], year

def find_header_row(file_path, sheet_name):
    temp_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    for i, row in temp_df.iterrows():
        row_str = row.astype(str).str.lower()
        if row_str.str.contains("name of the instrument").any():
            return i
    return None

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
                    df = pd.read_excel(file_path, sheet_name=sheet, header=header_row)
                    output_dir = os.path.join(OUTPUT_PATH, year, month)
                    os.makedirs(output_dir, exist_ok=True)
                    output_file = os.path.join(output_dir, f"{fund_name}_{month}_{year}.xlsx")
                    df.to_excel(output_file, index=False)
                    sheets_extracted += 1
                    logging.info(f"Saved -> {output_file}")
            except Exception as e:
                logging.error(f"Processing failed -> {file}")
                logging.error(str(e))
                error_count += 1

def main():
    setup_logging()
    logging.info("Starting SBI sheet splitting")
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