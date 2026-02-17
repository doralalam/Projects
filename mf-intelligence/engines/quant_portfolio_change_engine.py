import os
import pandas as pd
import re


# Paths
DATA_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/raw_files/quant"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/processed"

FUND = "quant"

CPLT_OUT_PATH = os.path.join(OUTPUT_PATH, FUND)

# Ensure output folder exists
os.makedirs(CPLT_OUT_PATH, exist_ok=True)

# Month order for sorting
MONTH_ORDER = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]




def extract_month_year(filename):
    """
    Extract month and year from filename
    Example: quant_Aggressive_Hybrid_Fund_Jan_2026.xlsx -> 'Jan_2026'
    """
    match = re.search(r'(' + '|'.join(MONTH_ORDER) + r')_(\d{4})', filename)
    if match:
        month, year = match.groups()
        return f"{month}_{year}" 
    else:
        return None

def load_equity(file_path, col_name):
    """
    Load Excel, skip metadata rows, filter equity, keep only ISIN, Stock Name, % to NAV
    """
    df = pd.read_excel(file_path, skiprows=7)
    df.columns = df.columns.str.strip()
    df_equity = df[df['ISIN'].str.startswith('INE', na=False)]
    df_equity = df_equity[['ISIN', 'NAME OF THE INSTRUMENT', '% to NAV']]
    df_equity = df_equity.rename(columns={'NAME OF THE INSTRUMENT':'Stock Name', '% to NAV': col_name})
    return df_equity

def get_files_for_fund(fund_folder):
    """
    Returns a sorted list of Excel files in the folder by year and month.
    Latest month will be last.
    """
    files = [f for f in os.listdir(fund_folder) if f.endswith(".xlsx")]
    
    def sort_key(filename):
        my = extract_month_year(filename)
        if not my:
            return (0, 0)  # put invalid files first
        month_str, year_str = my.split('_')
        month_idx = MONTH_ORDER.index(month_str)
        year_int = int(year_str)
        return (year_int, month_idx)
    
    files_sorted = sorted(files, key=sort_key)
    return files_sorted


# Main Execution

fund_folders = [f for f in os.listdir(DATA_PATH) if os.path.isdir(os.path.join(DATA_PATH, f))]

for fund in fund_folders:
    fund_path = os.path.join(DATA_PATH, fund)
    files = get_files_for_fund(fund_path)
    
    if len(files) < 3:
        print(f"Not enough files for {fund}. Skipping...")
        continue
    
    # Latest, previous month, quarter ago
    latest_file = os.path.join(fund_path, files[-1])
    prev_file = os.path.join(fund_path, files[-2])
    quarter_file = os.path.join(fund_path, files[-3])

    # Extract dynamic month-year column names
    latest_col = extract_month_year(files[-1])
    prev_col = extract_month_year(files[-2])
    quarter_col = extract_month_year(files[-3])

    # Load equity data
    df_latest = load_equity(latest_file, latest_col)
    df_prev = load_equity(prev_file, prev_col)
    df_quarter = load_equity(quarter_file, quarter_col)

    # Merge all three
    df_merged = pd.merge(df_latest, df_prev, on="ISIN", how="outer")
    df_merged = pd.merge(df_merged, df_quarter, on="ISIN", how="outer")

    # Consolidate Stock Name into one column
    df_merged['Stock Name'] = df_merged['Stock Name_x'].combine_first(df_merged['Stock Name_y']).combine_first(df_merged['Stock Name'])
    df_merged = df_merged.drop(columns=['Stock Name_x', 'Stock Name_y'])

    # Fill missing holdings with 0
    df_merged[[latest_col, prev_col, quarter_col]] = df_merged[[latest_col, prev_col, quarter_col]].fillna(0)

    # Calculate Month-on-Month and Quarter-on-Quarter changes
    df_merged['MoM'] = df_merged[latest_col] - df_merged[prev_col]
    df_merged['QoQ'] = df_merged[latest_col] - df_merged[quarter_col]

    # Add Mutual Fund Name column
    df_merged['Mutual Fund'] = fund.replace('_',' ')

    # Reorder columns
    df_final = df_merged[['ISIN','Stock Name','Mutual Fund', latest_col, prev_col, quarter_col,'MoM','QoQ']]

    # Sort by latest holding descending
    df_final = df_final.sort_values(by=latest_col, ascending=False)

    # Save output
    output_file = os.path.join(CPLT_OUT_PATH, f"{fund}_Equity_Holdings_Comparison.xlsx")
    df_final.to_excel(output_file, index=False)
    print(f"Processed {fund} -> {output_file} ({len(df_final)} rows)")

print("All funds processed successfully!")
