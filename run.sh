#!/usr/bin/env bash
# One clone-and-run command:  ./run.sh          (full scale, from config)
#                             ./run.sh sample   (tiny, for a quick smoke test)
set -euo pipefail
SCALE="${1:-full}"
export PYSPARK_PYTHON=python3

if [[ "$SCALE" != "sample" && "$SCALE" != "full" ]]; then
  echo "Usage: ./run.sh [sample|full]" >&2
  exit 2
fi

if ! command -v python >/dev/null 2>&1; then
  echo "Required command not found: python" >&2
  exit 1
fi
if ! java -version >/dev/null 2>&1; then
  echo "A working Java runtime is required for Spark. Install Java 11 and retry." >&2
  exit 1
fi

python - <<'PY'
import importlib.util
import sys

required = ("duckdb", "pyspark", "yaml", "dbt")
missing = [name for name in required if importlib.util.find_spec(name) is None]
if missing:
    print(
        "Missing Python dependencies: " + ", ".join(missing)
        + ". Install them with: python -m pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise SystemExit(1)
PY

echo "== 1/8 generate ($SCALE) ==";  python src/generate_synthetic_data.py "$SCALE"
echo "== 2/8 bronze ==";             python src/bronze_ingest.py
echo "== 3/8 silver ==";             python src/silver_transform.py
echo "== 4/8 dq gate ==";            python src/dq_checks.py
echo "== 5/8 gold ==";               python src/gold_model.py
echo "== 6/8 spark-sql (HiveQL) =="; python src/spark_sql_transforms.py
echo "== 7/8 warehouse ==";          python src/warehouse_load.py
echo "== 8/8 dbt marts + tests ==";  (cd dbt && dbt build --profiles-dir .)
echo "DONE. Gold warehouse at data/warehouse/discovery.duckdb ; dbt marts at dbt/dbt_warehouse.duckdb"
