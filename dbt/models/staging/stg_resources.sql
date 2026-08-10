select resource_id, service, app_id, account_id, private_ip
from read_parquet('{{ var("gold") }}/dim_resource/**/*.parquet')
