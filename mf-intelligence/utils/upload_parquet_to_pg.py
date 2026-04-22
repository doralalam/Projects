import pandas as pd
from sqlalchemy import create_engine

OUTPUT_FILE = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/master_dataset/all_amc/all_amc_clean.parquet"

df = pd.read_parquet(OUTPUT_FILE)

engine = create_engine("postgresql+psycopg2://postgres:1234@localhost:5433/mf_intelligence")

df.to_sql("amc_master", engine, if_exists="replace", index=False)

print("Data uploaded to PostgreSQL successfully")