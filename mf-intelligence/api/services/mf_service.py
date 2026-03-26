import pandas as pd
from db.db import get_conn

def fetch_table(table_name: str, limit: int = 100):
    conn = get_conn()

    query = f"""
        SELECT *
        FROM {table_name}
        ORDER BY total_diff DESC
        LIMIT %s
    """

    df = pd.read_sql(query, conn, params=(limit,))
    conn.close()

    return df.to_dict(orient="records")


def fetch_drilldown(isin: str):
    conn = get_conn()

    query = """
        SELECT fund, amc, diff, movement_type
        FROM mf_drilldown
        WHERE isin = %s
        ORDER BY diff DESC
    """

    df = pd.read_sql(query, conn, params=(isin,))
    conn.close()

    return df.to_dict(orient="records")