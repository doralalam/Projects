import os
import pandas as pd


BASE_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data"
AMC = "edelweiss"

PROCESSED_PATH = os.path.join(BASE_PATH, "processed", AMC)
MASTER_OUTPUT = os.path.join(BASE_PATH,"masters",AMC,f"{AMC}_master_report.xlsx")

os.makedirs(os.path.dirname(MASTER_OUTPUT), exist_ok=True)



all_dfs = []

for file in os.listdir(PROCESSED_PATH):

    if file.endswith("_Equity_Holdings_Comparison.xlsx"):

        df = pd.read_excel(
            os.path.join(PROCESSED_PATH, file)
        )

        all_dfs.append(df)


if not all_dfs:
    print("No processed files found.")
    exit()


full_df = pd.concat(all_dfs, ignore_index=True)



fixed_cols = [
    'ISIN',
    'Stock Name',
    'Industry',
    'Mutual Fund',
    'Status',
    'MoM',
    'QoQ'
]

month_cols = [
    col for col in full_df.columns
    if col not in fixed_cols
]


# Sort months → Latest first
month_cols_sorted = sorted(
    month_cols,
    key=lambda x: pd.to_datetime(
        x,
        format="%b_%Y"
    ),
    reverse=True
)


latest_col = month_cols_sorted[0]
prev_col = (
    month_cols_sorted[1]
    if len(month_cols_sorted) > 1
    else None
)

third_last_col = (
    month_cols_sorted[2]
    if len(month_cols_sorted) > 2
    else None
)


# Fill NaNs
full_df[month_cols] = full_df[month_cols].fillna(0)



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


def calculate_fund_conviction(latest, mom, qoq):

    mom_score = max(mom, 0)
    qoq_score = max(qoq, 0)

    conviction = (
        latest * 0.6
        + mom_score * 0.2
        + qoq_score * 0.2
    )

    return conviction


def calculate_amc_conviction(latest, mom, qoq, fund_count):

    mom_score = max(mom, 0)
    qoq_score = max(qoq, 0)

    conviction = (
        latest * 0.5
        + mom_score * 0.2
        + qoq_score * 0.2
        + fund_count * 2
    )

    return conviction



output_rows = []


stock_totals = (
    full_df
    .groupby(
        ['ISIN','Stock Name','Industry']
    )[latest_col]
    .sum()
    .reset_index()
)

stock_totals = stock_totals.sort_values(
    by=latest_col,
    ascending=False
)


for _, stock_row in stock_totals.iterrows():

    isin = stock_row['ISIN']
    stock_name = stock_row['Stock Name']
    industry = stock_row['Industry']

    group = full_df[
        (full_df['ISIN'] == isin) &
        (full_df['Stock Name'] == stock_name)
    ]



    total_latest = group[latest_col].sum()
    total_prev = (
        group[prev_col].sum()
        if prev_col else 0
    )

    total_third = (
        group[third_last_col].sum()
        if third_last_col else 0
    )

    mom_total = total_latest - total_prev
    qoq_total = total_latest - total_third

    fund_count = group[
        group[latest_col] > 0
    ].shape[0]


    status_total = determine_status(
        total_latest,
        total_prev,
        total_third,
        mom_total,
        qoq_total
    )


    conviction_total = calculate_amc_conviction(
        total_latest,
        mom_total,
        qoq_total,
        fund_count
    )



    output_rows.append({

        "ISIN": isin,
        "Stock Name": stock_name,
        "Industry": industry,
        "Mutual Fund": f"{AMC.title()} (Total)",
        "Status": status_total,
        "Conviction Score": round(conviction_total,2),
        "Latest %": total_latest,
        "Previous %": total_prev,
        "3rd Last %": total_third,
        "MoM": round(mom_total,2),
        "QoQ": round(qoq_total,2)
    })



    group_sorted = group.sort_values(
        by=latest_col,
        ascending=False
    )


    for _, row in group_sorted.iterrows():

        latest_val = row[latest_col]
        prev_val = row[prev_col] if prev_col else 0
        third_val = row[third_last_col] if third_last_col else 0

        mom = latest_val - prev_val
        qoq = latest_val - third_val


        status_fund = determine_status(
            latest_val,
            prev_val,
            third_val,
            mom,
            qoq
        )


        fund_conviction = calculate_fund_conviction(
            latest_val,
            mom,
            qoq
        )


        output_rows.append({

            "ISIN": "",
            "Stock Name": "",
            "Industry": "",
            "Mutual Fund": "   " + row['Mutual Fund'],
            "Status": status_fund,
            "Conviction Score": round(fund_conviction,2),
            "Latest %": latest_val,
            "Previous %": prev_val,
            "3rd Last %": third_val,
            "MoM": round(mom,2),
            "QoQ": round(qoq,2)
        })


final_df = pd.DataFrame(output_rows)


final_df = final_df[[
    "ISIN",
    "Stock Name",
    "Industry",
    "Mutual Fund",
    "Status",
    "Conviction Score",
    "Latest %",
    "Previous %",
    "3rd Last %",
    "MoM",
    "QoQ"
]]



final_df.to_excel(
    MASTER_OUTPUT,
    index=False
)


print("Master report created successfully!")
print(f"File saved at -> {MASTER_OUTPUT}")
print(f"Total rows -> {len(final_df)}")
