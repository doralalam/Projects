import pandas as pd
import logging
from pathlib import Path

BASE_DIR = Path("/Users/dorababulalam/GitHub/Projects/mf-intelligence")

INPUT_FILE = BASE_DIR / "data" / "master_dataset" / "all_amc" / "all_amc.parquet"

OUTPUT_FILE = BASE_DIR / "data" / "master_dataset" / "all_amc" / "all_amc_clean.parquet"

LOG_FILE = BASE_DIR / "logs" / "builders" / "all_amc_clean_builder.log"


def setup_logging():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )


def build_clean_dataset():

    logging.info("Loading dataset")

    df = pd.read_parquet(INPUT_FILE)

    month_map = {
        "Jan":1,"Feb":2,"Mar":3,"Apr":4,
        "May":5,"Jun":6,"Jul":7,"Aug":8,
        "Sep":9,"Oct":10,"Nov":11,"Dec":12
    }

    df["month_num"] = df["month"].map(month_map)

    df["report_date"] = pd.to_datetime(
        dict(year=df["year"], month=df["month_num"], day=1)
    )

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    df = df.drop(columns=["month_num"])

    df = df[
        [
            "amc",
            "fund",
            "stock",
            "isin",
            "sector",
            "quantity",
            "market_value",
            "weight",
            "report_date",
            "month",
            "year"
        ]
    ]

    logging.info("Saving clean parquet")

    df.to_parquet(OUTPUT_FILE, index=False)

    print("\nClean dataset created")
    print("---------------------")
    print("Rows:", len(df))
    print("Funds:", df["fund"].nunique())
    print("Stocks:", df["stock"].nunique())
    print("AMCs:", df["amc"].nunique())


def main():

    setup_logging()

    build_clean_dataset()


if __name__ == "__main__":
    main()