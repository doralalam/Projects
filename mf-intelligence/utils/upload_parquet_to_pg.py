import pandas as pd
from sqlalchemy import create_engine, text

OUTPUT_FILE = "/Users/dorababulalam/GitHub/Projects/mf-intelligence/data/master_dataset/all_amc/all_amc_clean.parquet"

df = pd.read_parquet(OUTPUT_FILE)

engine = create_engine("postgresql+psycopg2://postgres:1234@localhost:5433/mf_intelligence")

with engine.connect() as conn:
    conn.execute(text("TRUNCATE TABLE amc_master CASCADE;"))
    conn.commit()

df.to_sql("amc_master", engine, if_exists="append", index=False)

print("Data uploaded to PostgreSQL successfully!!!")