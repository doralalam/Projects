
DROP MATERIALIZED VIEW IF EXISTS mf_decreased;

CREATE MATERIALIZED VIEW mf_decreased AS
WITH summed AS (
    SELECT
        isin,
        MIN(stock) AS stock,
        MIN(sector) AS sector,
        SUM(net_change) AS change,
        COUNT(DISTINCT fund) AS fund_count,
        SUM(net_change) / COUNT(DISTINCT fund) AS avg_change,
        COUNT(*) FILTER (WHERE status = 'Fresh') AS fresh_count,
        COUNT(*) FILTER (WHERE status = 'Exit') AS exit_count
    FROM mf_classified
    GROUP BY isin
)
SELECT
    *,
    'Decreased' AS bucket
FROM summed
WHERE change < 0
ORDER BY change ASC;

select * from mf_decreased;