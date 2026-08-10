-- Network flows that cross an account boundary (potential exposure).
-- Pre-aggregate the 10M-row flow fact down to distinct IP edges BEFORE joining to
-- resources twice — this collapses ~10M rows to ~50K edges, so the double join and
-- final aggregation stay small (and never OOM the warehouse).
with edges as (
    select srcaddr, dstaddr,
           count(*)     as flow_count,
           sum(bytes)   as total_bytes
    from {{ ref('stg_flows') }}
    group by 1, 2
)
select s.account_id as src_account,
       d.account_id as dst_account,
       sum(e.flow_count)  as flow_count,
       sum(e.total_bytes) as total_bytes
from edges e
join {{ ref('stg_resources') }} s on e.srcaddr = s.private_ip
join {{ ref('stg_resources') }} d on e.dstaddr = d.private_ip
where s.account_id <> d.account_id
group by 1, 2
