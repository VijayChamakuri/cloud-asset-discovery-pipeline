#!/usr/bin/env bash
# One clone-and-run command:  ./run.sh          (full scale, from config)
#                             ./run.sh sample   (tiny, for a quick smoke test)
set -euo pipefail
SCALE="${1:-full}"
export PYSPARK_PYTHON=python3
pip install -q -r requirements.txt --break-system-packages 2>/dev/null || pip install -q -r requirements.txt
echo "== 1/8 generate ($SCALE) ==";  python src/generate_synthetic_data.py "$SCALE"
echo "== 2/8 bronze ==";             python src/bronze_ingest.py
echo "== 3/8 silver ==";             python src/silver_transform.py
echo "== 4/8 dq gate ==";            python src/dq_checks.py
echo "== 5/8 gold ==";               python src/gold_model.py
echo "== 6/8 spark-sql (HiveQL) ==";  python src/spark_sql_transforms.py
echo "== 7/8 warehouse ==";          python src/warehouse_load.py
echo "== 8/8 dbt marts + tests ==";  (cd dbt && dbt build --profiles-dir . 2>/dev/null) || echo "  (dbt optional; install dbt-core==1.7.17 dbt-duckdb==1.7.4 to enable)"
echo "DONE. Gold warehouse at data/warehouse/discovery.duckdb ; dbt marts at dbt/dbt_warehouse.duckdb"
