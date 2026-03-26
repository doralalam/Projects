
DROP MATERIALIZED VIEW IF EXISTS mf_increased;

CREATE MATERIALIZED VIEW mf_increased AS
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
    'Increased' AS bucket
FROM summed
WHERE change > 0   
ORDER BY change DESC;

select * from mf_increased;