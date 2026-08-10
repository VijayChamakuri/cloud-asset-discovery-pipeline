select principal_id, resource_id, event_name, error_code, dt
from read_parquet('{{ var("gold") }}/fact_api_action/**/*.parquet')
