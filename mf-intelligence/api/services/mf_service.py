import pandas as pd
from db.db import get_conn

def fetch_table(table_name: str, limit: int = 100):
    """
    Fetch top rows from any aggregated materialized view (mf_increased, mf_decreased, mf_fresh, mf_exit)
    Ordered by 'change' descending (or ascending if needed for decreases)
    """
    conn = get_conn()

    # For decreased table, we order ascending, otherwise descending
    order = "ASC" if "decreased" in table_name.lower() else "DESC"

    query = f"""
        SELECT *
        FROM {table_name}
        ORDER BY change {order}
        LIMIT %s
    """

    df = pd.read_sql(query, conn, params=(limit,))
    conn.close()

    return df.to_dict(orient="records")


def fetch_drilldown(isin: str):
    """
    Fetch detailed per-fund movement for a given ISIN from mf_drilldown
    Includes stock, sector, fund, AMC, net_change, and bucket
    """
    conn = get_conn()

    query = """
        SELECT fund, amc, stock, sector, change, bucket
        FROM mf_drilldown
        WHERE isin = %s
        ORDER BY change DESC
    """

    df = pd.read_sql(query, conn, params=(isin,))
    conn.close()

    return df.to_dict(orient="records")