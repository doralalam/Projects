
DROP MATERIALIZED VIEW IF EXISTS mf_classified;


CREATE MATERIALIZED VIEW mf_classified AS
WITH latest_two_months AS (

    SELECT DISTINCT report_date
    FROM amc_master
    ORDER BY report_date DESC
    LIMIT 2
),
filtered AS (

    SELECT *
    FROM amc_master
    WHERE report_date IN (SELECT report_date FROM latest_two_months)
),
weights_with_prev AS (

    SELECT
        amc,
        fund,
        isin,
        stock,
        sector,
        report_date,
        weight,
        LAG(weight) OVER (
            PARTITION BY amc, fund, isin
            ORDER BY report_date
        ) AS prev_weight
    FROM filtered
),
latest_only AS (

    SELECT *
    FROM weights_with_prev
    WHERE report_date = (SELECT MAX(report_date) FROM filtered)
)
SELECT
    amc,
    fund,
    isin,
    stock,
    sector,
    report_date AS latest_month,
    COALESCE(weight, 0) AS latest_weight,
    COALESCE(prev_weight, 0) AS previous_weight,
    COALESCE(weight, 0) - COALESCE(prev_weight, 0) AS net_change,
    CASE
        WHEN COALESCE(prev_weight, 0) = 0 AND COALESCE(weight, 0) > 0 THEN 'Fresh'
        WHEN COALESCE(weight, 0) = 0 AND COALESCE(prev_weight, 0) > 0 THEN 'Exit'
        WHEN COALESCE(weight, 0) > 0 AND COALESCE(prev_weight, 0) > 0 AND (weight - prev_weight) > 0 THEN 'Increased'
        WHEN COALESCE(weight, 0) > 0 AND COALESCE(prev_weight, 0) > 0 AND (weight - prev_weight) < 0 THEN 'Decreased'
        ELSE 'No Change'
    END AS status
FROM latest_only;

select * from mf_classified where stock = 'Angel One ' and fund = 'Invesco India Smallcap Fund';