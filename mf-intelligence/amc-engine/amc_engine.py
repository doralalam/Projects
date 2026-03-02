import os
import pandas as pd




BASE_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data"
MASTERS_PATH = os.path.join(BASE_PATH, "masters")
OUTPUT_FILE = os.path.join(BASE_PATH, "masters", "all-AMC", "all_amc_master_report.xlsx")




amc_data = {}

for amc in os.listdir(MASTERS_PATH):

    amc_folder = os.path.join(MASTERS_PATH, amc)

    if not os.path.isdir(amc_folder):
        continue

    file_path = os.path.join(amc_folder, f"{amc}_master_report.xlsx")

    if not os.path.exists(file_path):
        continue

    df = pd.read_excel(file_path)

    if df.empty:
        continue

    amc_data[amc.title()] = df.reset_index(drop=True)


if not amc_data:
    print("No AMC master files found.")
    exit()



structured = {}

for amc_name, df in amc_data.items():

    i = 0

    while i < len(df):

        row = df.iloc[i]

        isin = str(row.get("ISIN", "")).strip()

        # Checking ISIN
        if not isin or isin.lower() == "nan":
            i += 1
            continue

        amc_row = row.to_dict()
        fund_rows = []

        j = i + 1

        while j < len(df):

            next_row = df.iloc[j]
            next_isin = str(next_row.get("ISIN", "")).strip()

            if next_isin and next_isin.lower() != "nan":
                break

            fund_rows.append(next_row.to_dict())
            j += 1

        if isin not in structured:
            structured[isin] = {}

        structured[isin][amc_name] = {
            "amc_row": amc_row,
            "fund_rows": fund_rows
        }

        i = j




final_rows = []

for isin, amc_blocks in structured.items():

    # Safe sorting (handles blanks & strings)
    sorted_amcs = sorted(
        amc_blocks.items(),
        key=lambda x: pd.to_numeric(
            x[1]["amc_row"].get("Current Allocation (%)", 0),
            errors="coerce"
        ) if x[1]["amc_row"].get("Current Allocation (%)") is not None else 0,
        reverse=True
    )

    first_amc = True

    for amc_name, block in sorted_amcs:

        amc_row = block["amc_row"].copy()

        # Show ISIN details only once
        if not first_amc:
            amc_row["ISIN"] = ""
            amc_row["Company Name"] = ""
            amc_row["Sector"] = ""

        final_rows.append(amc_row)

        first_amc = False

        # Add fund rows
        for fund in block["fund_rows"]:

            fund_copy = fund.copy()
            fund_copy["ISIN"] = ""
            fund_copy["Company Name"] = ""
            fund_copy["Sector"] = ""

            final_rows.append(fund_copy)




final_df = pd.DataFrame(final_rows)




numeric_cols = [
    "Current Allocation (%)",
    "Previous Allocation (%)",
    "Quarter Allocation (%)",
    "MoM Change (%)",
    "QoQ Change (%)"
]

for col in numeric_cols:
    if col in final_df.columns:
        final_df[col] = pd.to_numeric(final_df[col], errors="coerce")




os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

final_df.to_excel(OUTPUT_FILE, index=False)

print("ALL AMC MASTER CREATED SUCCESSFULLY")
print(f"Saved at -> {OUTPUT_FILE}")
print(f"Total rows -> {len(final_df)}")