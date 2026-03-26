DROP MATERIALIZED VIEW IF EXISTS mom_changes;
CREATE MATERIALIZED VIEW mom_changes AS
SELECT 
    a.amc,
    a.fund,
    a.stock,
    a.isin,
    a.sector,
    a.report_date,
    a.weight AS current_weight,
    b.weight AS prev_weight,
    ROUND(
        (
            CASE 
                WHEN b.weight IS NULL OR b.weight = 0 THEN NULL
                ELSE ((a.weight - b.weight) / b.weight) * 100
            END
        )::numeric
    , 2) AS mom_change_pct,
    ROUND((a.weight - COALESCE(b.weight, 0))::numeric, 4) AS mom_change_abs,
    CASE 
        WHEN b.weight IS NULL THEN 'NEW'
        WHEN a.weight = 0 THEN 'EXIT'
        WHEN a.weight > b.weight THEN 'INCREASED'
        WHEN a.weight < b.weight THEN 'DECREASED'
        ELSE 'NO_CHANGE'
    END AS signal
FROM amc_master a
LEFT JOIN amc_master b
ON a.fund = b.fund
AND a.stock = b.stock
AND a.report_date = b.report_date + INTERVAL '1 month';