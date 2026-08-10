-- Optimized analytics SQL over the gold warehouse (Redshift/Athena-compatible).
-- Q1: Cross-account network exposure — accounts talking to accounts they should not.
SELECT src.account_id AS src_account, dst.account_id AS dst_account,
       count(*) AS flow_count, sum(f.bytes) AS bytes
FROM fact_network_flow f
JOIN dim_resource src ON f.srcaddr = src.private_ip
JOIN dim_resource dst ON f.dstaddr = dst.private_ip
WHERE src.account_id <> dst.account_id
GROUP BY 1,2
ORDER BY bytes DESC
LIMIT 50;

-- Q2: Application blast radius — how many resources & accounts each discovered app spans.
SELECT m.discovered_app_id,
       count(DISTINCT m.resource_id) AS resources,
       count(DISTINCT r.account_id)  AS accounts
FROM resource_application_map m
JOIN dim_resource r USING (resource_id)
GROUP BY 1
ORDER BY resources DESC
LIMIT 25;

-- Q3: Suspicious principals — high AccessDenied rate (possible recon/abuse).
SELECT principal_id,
       count(*) AS actions,
       sum(CASE WHEN error_code = 'AccessDenied' THEN 1 ELSE 0 END) AS denied,
       round(100.0*sum(CASE WHEN error_code='AccessDenied' THEN 1 ELSE 0 END)/count(*),2) AS denied_pct
FROM fact_api_action
GROUP BY 1
HAVING count(*) > 5
ORDER BY denied_pct DESC
LIMIT 25;
