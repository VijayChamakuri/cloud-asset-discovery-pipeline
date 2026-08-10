-- How many resources and accounts each discovered application spans.
select m.discovered_app_id,
       count(distinct m.resource_id) as resources,
       count(distinct r.account_id)  as accounts
from {{ ref('stg_appmap') }} m
join {{ ref('stg_resources') }} r using (resource_id)
group by 1
