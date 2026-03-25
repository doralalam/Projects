from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv


load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

@app.get("/fund/{fund_name}/mom-pivot")
def get_mom_pivot(fund_name: str):
    try:
        query = """
        SELECT stock, sector, report_date, current_weight, mom_change_pct
        FROM mom_changes
        WHERE fund ILIKE %s
        """
        df = pd.read_sql(query, conn, params=[f"%{fund_name}%"])

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

            if key not in pivot_data:
                pivot_data[key] = {}

            pivot_data[key][f"{label}_weight"] = None if pd.isna(row["current_weight"]) else float(row["current_weight"])
            pivot_data[key][f"{label}_mom"] = None if pd.isna(row["mom_change"]) else float(row["mom_change"])

        result = []

        for (stock, sector), values in pivot_data.items():
            row = {"stock": stock, "sector": sector}
            for m in months:
                label = m.to_timestamp().strftime("%b %Y")
                row[f"{label}_weight"] = values.get(f"{label}_weight", None)
                row[f"{label}_mom"] = values.get(f"{label}_mom", None)
            result.append(row)

        return result

    except Exception as e:
        return {"error": str(e)}