"""Airflow DAG orchestrating the pipeline: generate -> bronze -> silver -> DQ gate
-> gold -> warehouse, with an SNS/SQS-style completion notification.
Owner: Data Engineer agent. (Valid, importable DAG; runs each stage as a task.)"""
from datetime import datetime, timedelta
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    HAS_AIRFLOW = True
except Exception:
    HAS_AIRFLOW = False

default_args = {"owner": "data-eng", "retries": 2,
                "retry_delay": timedelta(minutes=5), "depends_on_past": False}

def notify(**_):
    # Real boto3 SNS publish (see src/notify.py). If no TopicArn is configured
    # (e.g. local dev), log instead of failing the run.
    import os, sys
    sys.path.insert(0, os.path.join(os.environ.get("PROJECT", "."), "src"))
    from notify import publish_completion
    topic = os.environ.get("COMPLETION_TOPIC_ARN")
    if topic:
        mid = publish_completion(topic, {"status": "SUCCESS", "pipeline": "asset_discovery"})
        print(f"[notify] published SNS message {mid}")
    else:
        print("[notify] COMPLETION_TOPIC_ARN unset; skipping publish (local dev)")

if HAS_AIRFLOW:
    with DAG("asset_discovery", default_args=default_args,
             schedule_interval="0 3 * * *", start_date=datetime(2026,1,1),
             catchup=False, max_active_runs=1, tags=["security","discovery"]) as dag:
        gen     = BashOperator(task_id="generate",  bash_command="cd $PROJECT && python src/generate_synthetic_data.py full")
        bronze  = BashOperator(task_id="bronze",    bash_command="cd $PROJECT && python src/bronze_ingest.py")
        silver  = BashOperator(task_id="silver",    bash_command="cd $PROJECT && python src/silver_transform.py")
        dq      = BashOperator(task_id="dq_gate",   bash_command="cd $PROJECT && python src/dq_checks.py")  # fails DAG if DQ fails
        gold    = BashOperator(task_id="gold",      bash_command="cd $PROJECT && python src/gold_model.py")
        sparksql= BashOperator(task_id="spark_sql", bash_command="cd $PROJECT && python src/spark_sql_transforms.py")
        wh      = BashOperator(task_id="warehouse", bash_command="cd $PROJECT && python src/warehouse_load.py")
        dbt     = BashOperator(task_id="dbt_marts", bash_command="cd $PROJECT/dbt && dbt build --profiles-dir .")
        done    = PythonOperator(task_id="notify",  python_callable=notify)
        gen >> bronze >> silver >> dq >> gold >> sparksql >> wh >> dbt >> done
