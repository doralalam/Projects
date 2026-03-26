
DROP MATERIALIZED VIEW IF EXISTS mf_fresh;


CREATE MATERIALIZED VIEW mf_fresh AS
WITH filtered AS (
    SELECT *
    FROM mf_classified
    WHERE status = 'Fresh'       
)
SELECT
    isin,
    MIN(stock) AS stock,
    MIN(sector) AS sector,
    SUM(net_change) AS change,                
    COUNT(DISTINCT fund) AS fund_count,
    SUM(net_change) / COUNT(DISTINCT fund) AS avg_change,
    COUNT(*) AS fresh_count,
    'Fresh' AS bucket                          
FROM filtered
GROUP BY isin
ORDER BY change DESC;

SELECT * FROM mf_fresh where stock = 'Angel One Limited';