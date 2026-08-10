select resource_id, discovered_app_id, ground_truth_app
from read_parquet('{{ var("gold") }}/resource_application_map/**/*.parquet')
