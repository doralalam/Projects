
DROP MATERIALIZED VIEW IF EXISTS mf_exit;


CREATE MATERIALIZED VIEW mf_exit AS
WITH filtered AS (
    SELECT *
    FROM mf_classified
    WHERE status = 'Exit'       
)
SELECT
    isin,
    MIN(stock) AS stock,
    MIN(sector) AS sector,
    SUM(net_change) AS change,                 
    COUNT(DISTINCT fund) AS fund_count,
    SUM(net_change) / COUNT(DISTINCT fund) AS avg_change,
    COUNT(*) AS exit_count,
    'Exit' AS bucket                           
FROM filtered
GROUP BY isin
ORDER BY change ASC;                           

SELECT * FROM mf_exit;