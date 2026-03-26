
DROP MATERIALIZED VIEW IF EXISTS mf_drilldown;


CREATE MATERIALIZED VIEW mf_drilldown AS
SELECT
    isin,
    stock,
    fund,
    amc,
    sector,
    net_change AS change,   
    status AS bucket            
FROM mf_classified
WHERE status != 'No Change';    

SELECT * 
FROM mf_drilldown;