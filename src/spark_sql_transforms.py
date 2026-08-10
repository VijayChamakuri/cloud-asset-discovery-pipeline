"""HiveQL-compatible transformations executed on Spark SQL.

Spark SQL implements the HiveQL dialect (same DDL, window functions, and built-ins),
so these queries run unchanged on Hive/EMR. We register the gold tables as SQL views
and build two analytical tables with CTEs + window functions:
  - principal_risk_ranked : rank IAM principals by AccessDenied rate (recon signal)
  - resource_flow_ranked  : rank resources by outbound bytes within their account
Owner: Analytics/Modeling agent."""
from utils import spark_session, load_cfg

HIVEQL_PRINCIPAL_RISK = """
WITH agg AS (
  SELECT principal_id,
         COUNT(*)                                              AS actions,
         SUM(CASE WHEN error_code = 'AccessDenied' THEN 1 ELSE 0 END) AS denied
  FROM fact_api_action
  GROUP BY principal_id
)
SELECT principal_id, actions, denied,
       ROUND(100.0 * denied / actions, 2)                      AS denied_pct,
       RANK() OVER (ORDER BY 1.0 * denied / actions DESC)      AS risk_rank
FROM agg
WHERE actions > 5
"""

HIVEQL_RESOURCE_FLOW = """
SELECT r.account_id, r.resource_id, r.service,
       SUM(f.bytes)                                                    AS out_bytes,
       RANK() OVER (PARTITION BY r.account_id ORDER BY SUM(f.bytes) DESC) AS bytes_rank_in_account
FROM fact_network_flow f
JOIN dim_resource r ON f.srcaddr = r.private_ip
GROUP BY r.account_id, r.resource_id, r.service
"""

def main():
    cfg = load_cfg(); spark = spark_session("spark-sql", cfg); spark.sparkContext.setLogLevel("ERROR")
    gold = cfg["paths"]["gold"]
    spark.read.parquet(f"{gold}/fact_api_action").createOrReplaceTempView("fact_api_action")
    spark.read.parquet(f"{gold}/fact_network_flow").createOrReplaceTempView("fact_network_flow")
    spark.read.parquet(f"{gold}/dim_resource").createOrReplaceTempView("dim_resource")

    pr = spark.sql(HIVEQL_PRINCIPAL_RISK)
    pr.write.mode("overwrite").parquet(f"{gold}/principal_risk_ranked")
    rf = spark.sql(HIVEQL_RESOURCE_FLOW)
    rf.write.mode("overwrite").parquet(f"{gold}/resource_flow_ranked")

    print(f"[spark-sql] principal_risk_ranked={pr.count()} resource_flow_ranked={rf.count()}")
    print("[spark-sql] top-risk principals (HiveQL window RANK):")
    for row in pr.orderBy("risk_rank").limit(3).collect():
        print("   ", row["principal_id"], "denied_pct=", row["denied_pct"], "rank=", row["risk_rank"])
    spark.stop()

if __name__ == "__main__":
    main()
