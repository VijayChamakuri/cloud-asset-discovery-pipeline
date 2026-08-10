-- Network flows that cross an account boundary (potential exposure).
select s.account_id as src_account,
       d.account_id as dst_account,
       count(*)      as flow_count,
       sum(f.bytes)  as total_bytes
from {{ ref('stg_flows') }} f
join {{ ref('stg_resources') }} s on f.srcaddr = s.private_ip
join {{ ref('stg_resources') }} d on f.dstaddr = d.private_ip
where s.account_id <> d.account_id
group by 1, 2
