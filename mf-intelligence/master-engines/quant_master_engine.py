import os
import pandas as pd

# Paths
BASE_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data"
AMC = "quant"

PROCESSED_PATH = os.path.join(BASE_PATH, "processed", AMC)
OUTPUT_FILE = os.path.join(BASE_PATH, "masters", AMC, f"{AMC}_master_report.xlsx")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)


def format_fund_name(name):

    abbreviations = {"ESG", "ETF", "PSU", "FOF", "ELSS"}

    words = name.split()
    formatted_words = []

    for word in words:
        if word.upper() in abbreviations:
            formatted_words.append(word.upper())
        else:
            formatted_words.append(word.capitalize())

    return " ".join(formatted_words)


# Load processed files
files = [
    f for f in os.listdir(PROCESSED_PATH)
    if f.endswith("_Equity_Holdings_Comparison.xlsx")
]

if len(files) == 0:
    print("No processed fund files found.")
    exit()

df_list = []

for file in files:
    file_path = os.path.join(PROCESSED_PATH, file)
    df = pd.read_excel(file_path)

    if df.empty:
        continue

    df_list.append(df)

if len(df_list) == 0:
    print("All processed files are empty.")
    exit()

df_master = pd.concat(df_list, ignore_index=True)

fixed_cols = ['ISIN','Stock Name','Industry','Mutual Fund','Status','MoM','QoQ']

month_cols = [
    col for col in df_master.columns
    if col not in fixed_cols
]

month_cols_sorted = sorted(month_cols, reverse=True)

latest_col = month_cols_sorted[0]
prev_col = month_cols_sorted[1] if len(month_cols_sorted) > 1 else None
third_last_col = month_cols_sorted[2] if len(month_cols_sorted) > 2 else None

df_master[month_cols] = df_master[month_cols].fillna(0)


def determine_status(latest, prev, quarter, mom, qoq):

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



stock_totals = (
    df_master
    .groupby(['ISIN','Stock Name','Industry'])[latest_col]
    .sum()
    .reset_index()
    .sort_values(by=latest_col, ascending=False)
)

output_rows = []

for _, stock_row in stock_totals.iterrows():

    isin = stock_row['ISIN']
    stock = stock_row['Stock Name']
    industry = stock_row['Industry']

    group = df_master[
        (df_master['ISIN'] == isin) &
        (df_master['Stock Name'] == stock)
    ]

    total_latest = group[latest_col].sum()
    total_prev = group[prev_col].sum() if prev_col else 0
    total_third = group[third_last_col].sum() if third_last_col else 0

    mom_total = total_latest - total_prev
    qoq_total = total_latest - total_third

    status_total = determine_status(
        total_latest,
        total_prev,
        total_third,
        mom_total,
        qoq_total
    )

    # -------- TOTAL ROW --------
    output_rows.append({
        "ISIN": isin,
        "Stock Name": stock,
        "Industry": industry,
        "Mutual Fund": format_fund_name(f"{AMC} mutual fund"),
        "Status": status_total,
        "Latest %": total_latest,
        "Previous %": total_prev,
        "3rd Last %": total_third,
        "MoM": mom_total,
        "QoQ": qoq_total
    })

    # -------- FUND LEVEL ROWS --------
    group_sorted = group.sort_values(by=latest_col, ascending=False)

    for _, row in group_sorted.iterrows():

        latest_val = row[latest_col]
        prev_val = row[prev_col]
        third_val = row[third_last_col]

        mom = latest_val - prev_val
        qoq = latest_val - third_val

        status_fund = determine_status(
            latest_val,
            prev_val,
            third_val,
            mom,
            qoq
        )

        formatted_name = format_fund_name(row['Mutual Fund'])

        output_rows.append({
            "ISIN": "",
            "Stock Name": "",
            "Industry": "",
            "Mutual Fund": "   " + formatted_name,
            "Status": status_fund,
            "Latest %": latest_val,
            "Previous %": prev_val,
            "3rd Last %": third_val,
            "MoM": mom,
            "QoQ": qoq
        })


final_df = pd.DataFrame(output_rows)


final_df = final_df.rename(columns={
    "Stock Name": "Company Name",
    "Industry": "Sector",
    "Mutual Fund": "Fund Name",
    "Status": "Holding Action",
    "Latest %": "Current Allocation (%)",
    "Previous %": "Previous Allocation (%)",
    "3rd Last %": "Quarter Allocation (%)",
    "MoM": "MoM Change (%)",
    "QoQ": "QoQ Change (%)"
})

final_df.to_excel(OUTPUT_FILE, index=False)

print("Master report created successfully!")
print(f"File: {OUTPUT_FILE}")
print(f"Rows: {len(final_df)}")