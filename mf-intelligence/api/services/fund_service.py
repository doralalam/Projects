import pandas as pd
from db.db import get_conn

def fetch_funds_list():
    conn = get_conn()

    query = """
    SELECT DISTINCT amc, fund 
    FROM mom_changes 
    ORDER BY amc, fund
    """

    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        return {}

    fund_map = {}
    for _, row in df.iterrows():
        fund_map.setdefault(row["amc"], []).append(row["fund"])

    return fund_map


def fetch_mom_pivot(amc: str, fund_name: str):
    conn = get_conn()

    query = """
    SELECT amc, stock, sector, report_date, current_weight, mom_change_pct
    FROM mom_changes
    WHERE amc ILIKE %s AND fund ILIKE %s
    """

    df = pd.read_sql(query, conn, params=[amc, fund_name])
    conn.close()

    if df.empty:
        return {"message": "No data found"}

    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
    df = df.dropna(subset=["report_date"])

    if df.empty:
        return {"message": "No valid dates"}

    df["current_weight"] = pd.to_numeric(df["current_weight"], errors="coerce")
    df["mom_change"] = pd.to_numeric(df["mom_change_pct"], errors="coerce")

    latest_date = df["report_date"].max()
    df = df[df["report_date"] >= latest_date - pd.DateOffset(months=11)]

    df = df.sort_values(by="report_date", ascending=False)
    months = sorted(df["report_date"].dt.to_period("M").unique(), reverse=True)

    pivot_data = {}

    for _, row in df.iterrows():
        key = (str(row["stock"]), str(row["sector"]))
        label = row["report_date"].strftime("%b %Y")

        pivot_data.setdefault(key, {})
        pivot_data[key][f"{label}_weight"] = None if pd.isna(row["current_weight"]) else float(row["current_weight"])
        pivot_data[key][f"{label}_mom"] = None if pd.isna(row["mom_change"]) else float(row["mom_change"])

    result = []

    for (stock, sector), values in pivot_data.items():
        row = {"stock": stock, "sector": sector}

        for m in months:
            label = m.to_timestamp().strftime("%b %Y")
            row[f"{label}_weight"] = values.get(f"{label}_weight")
            row[f"{label}_mom"] = values.get(f"{label}_mom")

        result.append(row)

    return result