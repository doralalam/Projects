import os
import pandas as pd




BASE_PATH = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data"
MASTERS_PATH = os.path.join(BASE_PATH, "masters")
OUTPUT_FILE = os.path.join(BASE_PATH, "masters", "all-AMC", "all_amc_master_report.xlsx")



amc_data = {}

for amc in os.listdir(MASTERS_PATH):

    amc_folder = os.path.join(MASTERS_PATH, amc)

    # Skip non-folders
    if not os.path.isdir(amc_folder):
        continue

    file_path = os.path.join(amc_folder, f"{amc}_master_report.xlsx")

    # Skip if file not present
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
        isin = str(row["ISIN"]).strip()

        # Skip blank rows (fund rows)
        if isin == "" or isin.lower() == "nan":
            i += 1
            continue

        # AMC total row
        amc_row = row.to_dict()
        fund_rows = []

        j = i + 1

        # Collect fund rows under this AMC
        while j < len(df):

            next_row = df.iloc[j]
            next_isin = str(next_row["ISIN"]).strip()

            # Stop when next ISIN block starts
            if next_isin != "" and next_isin.lower() != "nan":
                break

            fund_rows.append(next_row.to_dict())
            j += 1

        # Store structured block
        if isin not in structured:
            structured[isin] = {}

        structured[isin][amc_name] = {
            "amc_row": amc_row,
            "fund_rows": fund_rows
        }

        i = j




final_rows = []

for isin in structured.keys():

    amc_blocks = structured[isin]

    # Sort AMCs by Current Allocation descending
    sorted_amcs = sorted(
        amc_blocks.items(),
        key=lambda x: pd.to_numeric(
            x[1]["amc_row"].get("Current Allocation (%)", 0),
            errors="coerce"
        ),
        reverse=True
    )

    first_amc = True

    for amc_name, block in sorted_amcs:

        amc_row = block["amc_row"].copy()

        # Show ISIN details only once per ISIN
        if not first_amc:
            amc_row["ISIN"] = ""
            amc_row["Company Name"] = ""
            amc_row["Sector"] = ""

        final_rows.append(amc_row)

        first_amc = False

        # Add fund rows under AMC
        for fund in block["fund_rows"]:

            fund_copy = fund.copy()
            fund_copy["ISIN"] = ""
            fund_copy["Company Name"] = ""
            fund_copy["Sector"] = ""

            final_rows.append(fund_copy)




final_df = pd.DataFrame(final_rows)


# Ensure numeric columns remain numeric
numeric_cols = [
    "Current Allocation (%)",
    "Previous Allocation (%)",
    "Quarter Allocation (%)",
    "MoM Change (%)",
    "QoQ Change (%)"
]

for col in numeric_cols:
    if col in final_df.columns:
        final_df[col] = pd.to_numeric(final_df[col], errors="ignore")




# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

final_df.to_excel(OUTPUT_FILE, index=False)

print("ALL AMC MASTER CREATED SUCCESSFULLY")
print(f"Saved at -> {OUTPUT_FILE}")
print(f"Total rows -> {len(final_df)}")