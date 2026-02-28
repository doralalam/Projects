import os
import pandas as pd
import re


DATA_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/separated_files/sbi"
OUTPUT_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/processed/sbi"

os.makedirs(OUTPUT_PATH, exist_ok=True)

MONTH_ORDER = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]


def extract_month_year(filename):

    match = re.search(
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[_\-\s](\d{4})',
        filename,
        re.IGNORECASEc
    )

    if match:
        month = match.group(1).title()
        year = match.group(2)
        return f"{month}_{year}"

    return None


# --------------------------------------------------
# Load & Clean SBI Equity
# --------------------------------------------------
def load_equity(file_path, col_name):

    temp_df = pd.read_excel(file_path, header=None)

    header_row = None

    for i in range(len(temp_df)):
        row_values = temp_df.iloc[i].astype(str).str.lower()

        if any("name of the instrument" in str(val) for val in row_values):
            header_row = i
            break

    if header_row is None:
        raise Exception("Header row not found")

    df = pd.read_excel(file_path, header=header_row)
    df.columns = df.columns.str.strip()

    isin_col = None
    name_col = None
    weight_col = None
    industry_col = None

    for col in df.columns:

        col_lower = col.lower()

        if "isin" in col_lower:
            isin_col = col

        elif "name of the instrument" in col_lower:
            name_col = col

        elif "% to aum" in col_lower:
            weight_col = col

        elif "rating / industry" in col_lower:
            industry_col = col

    if not isin_col:
        raise Exception("ISIN column not found")

    if not name_col:
        raise Exception("Name column not found")

    if not weight_col:
        raise Exception("Weight column not found")


    # Clean % column
    df[weight_col] = (
        df[weight_col]
        .astype(str)
        .str.replace("%", "", regex=False)
    )

    df[weight_col] = pd.to_numeric(df[weight_col], errors="coerce")


    # Keep only valid equity ISINs
    df_equity = df[
        df[isin_col]
        .astype(str)
        .str.match(r'^INE.*\d{5}$', na=False)
    ]


    columns_needed = [isin_col, name_col, weight_col]

    if industry_col:
        columns_needed.append(industry_col)

    df_equity = df_equity[columns_needed]


    rename_map = {
        isin_col: "ISIN",
        name_col: "Stock Name",
        weight_col: col_name
    }

    if industry_col:
        rename_map[industry_col] = "Industry"

    df_equity = df_equity.rename(columns=rename_map)

    return df_equity


# --------------------------------------------------
# Sort files chronologically
# --------------------------------------------------
def get_files_for_fund(fund_folder):

    files = [f for f in os.listdir(fund_folder) if f.endswith(".xlsx")]

    def sort_key(filename):

        my = extract_month_year(filename)

        if not my:
            return (0,0)

        m,y = my.split("_")
        return (int(y), MONTH_ORDER.index(m))

    return sorted(files, key=sort_key)


# --------------------------------------------------
# Determine Holding Status
# --------------------------------------------------
def determine_status(row, latest_col, prev_col, quarter_col):

    latest = row[latest_col]
    prev = row[prev_col]
    quarter = row[quarter_col]

    mom = row['MoM']
    qoq = row['QoQ']

    if prev == 0 and latest > 0:
        return "Fresh Entry"

    elif latest == 0 and (prev > 0 or quarter > 0):
        return "Complete Exit"

    elif mom > 0 and qoq > 0:
        return "Adding Consistently"

    elif mom > 0:
        return "Adding"

    elif mom < 0 and qoq < 0:
        return "Reducing Consistently"

    elif mom < 0:
        return "Reducing"

    else:
        return "Stable"


# --------------------------------------------------
# Process all SBI Funds
# --------------------------------------------------
fund_folders = [
    f for f in os.listdir(DATA_PATH)
    if os.path.isdir(os.path.join(DATA_PATH, f))
]

for fund in fund_folders:

    fund_path = os.path.join(DATA_PATH, fund)
    files = get_files_for_fund(fund_path)

    if len(files) < 2:
        print(f"Not enough files for {fund}. Skipping...")
        continue

    file_months = [
        (f, extract_month_year(f))
        for f in files
        if extract_month_year(f)
    ]

    if len(file_months) < 2:
        print(f"Not enough valid files for {fund}. Skipping...")
        continue

    latest_file, latest_col = file_months[-1]
    prev_file, prev_col = file_months[-2]

    if len(file_months) >= 4:
        quarter_file, quarter_col = file_months[-4]
    else:
        quarter_file, quarter_col = file_months[0]

    latest_file = os.path.join(fund_path, latest_file)
    prev_file = os.path.join(fund_path, prev_file)
    quarter_file = os.path.join(fund_path, quarter_file)

    df_latest = load_equity(latest_file, latest_col)
    df_prev = load_equity(prev_file, prev_col)
    df_quarter = load_equity(quarter_file, quarter_col)

    df_merged = pd.merge(df_latest, df_prev, on="ISIN", how="outer", suffixes=("_latest", "_prev"))
    df_merged = pd.merge(df_merged, df_quarter, on="ISIN", how="outer")

    df_merged['Stock Name'] = (
        df_merged.get('Stock Name_latest')
        .combine_first(df_merged.get('Stock Name_prev'))
        .combine_first(df_merged.get('Stock Name'))
    )

    if 'Industry_latest' in df_merged.columns:
        df_merged['Industry'] = (
            df_merged.get('Industry_latest')
            .combine_first(df_merged.get('Industry_prev'))
            .combine_first(df_merged.get('Industry'))
        )

    df_merged[[latest_col, prev_col, quarter_col]] = (
        df_merged[[latest_col, prev_col, quarter_col]]
        .fillna(0)
    )

    df_merged['MoM'] = df_merged[latest_col] - df_merged[prev_col]
    df_merged['QoQ'] = df_merged[latest_col] - df_merged[quarter_col]

    df_merged['Mutual Fund'] = fund.replace("_"," ")

    df_merged['Status'] = df_merged.apply(
        lambda row: determine_status(row, latest_col, prev_col, quarter_col),
        axis=1
    )

    final_columns = [
        'ISIN',
        'Stock Name',
        'Industry',
        'Mutual Fund',
        'Status',
        latest_col,
        prev_col,
        quarter_col,
        'MoM',
        'QoQ'
    ]

    df_final = df_merged[final_columns]
    df_final = df_final.sort_values(by=latest_col, ascending=False)

    output_file = os.path.join(
        OUTPUT_PATH,
        f"{fund}_Equity_Holdings_Comparison.xlsx"
    )

    df_final.to_excel(output_file, index=False)

    print(f"Processed {fund} -> {len(df_final)} rows")

print("\nAll SBI funds processed successfully!")