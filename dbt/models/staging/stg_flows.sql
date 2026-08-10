select account_id, srcaddr, dstaddr, srcport, dstport, bytes, action, dt
from read_parquet('{{ var("gold") }}/fact_network_flow/**/*.parquet')
