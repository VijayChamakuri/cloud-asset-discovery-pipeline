-- Principals with an elevated AccessDenied rate (recon / abuse signal).
select principal_id,
       count(*)                                                   as actions,
       sum(case when error_code = 'AccessDenied' then 1 else 0 end) as denied,
       round(100.0 * sum(case when error_code = 'AccessDenied' then 1 else 0 end) / count(*), 2) as denied_pct
from {{ ref('stg_api') }}
group by 1
having count(*) > 5
