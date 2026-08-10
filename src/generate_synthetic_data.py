"""
Generate SYNTHETIC source data for the pipeline.

PROVENANCE (honest): This data is 100% synthetic. It is generated to match the
PUBLIC field schemas of AWS VPC Flow Logs (v2) and AWS CloudTrail, plus a simple
cloud resource inventory. It is NOT real production data and contains no real
customer, account, or security information. It exists only to exercise the
pipeline at realistic record scale.

Owner: Data Engineer agent.
"""
import sys
from pyspark.sql import functions as F
from utils import spark_session, load_cfg

SERVICES = ["ec2", "s3", "rds", "lambda", "dynamodb", "eks", "elb", "redshift"]
APP_TAGS = [f"app-{i:03d}" for i in range(120)]   # ground-truth application each asset belongs to
ACTIONS  = ["AssumeRole", "GetObject", "PutObject", "RunInstances",
            "DescribeInstances", "CreateUser", "AuthorizeSecurityGroupIngress",
            "GetSecretValue", "Decrypt", "ListBuckets"]

def gen_resources(spark, n):
    df = (spark.range(n).withColumnRenamed("id", "rid")
          .withColumn("resource_id", F.concat(F.lit("r-"), F.col("rid")))
          .withColumn("service", F.element_at(F.array(*[F.lit(s) for s in SERVICES]),
                       (F.col("rid") % len(SERVICES) + 1).cast("int")))
          .withColumn("app_id", F.element_at(F.array(*[F.lit(a) for a in APP_TAGS]),
                       (F.col("rid") % len(APP_TAGS) + 1).cast("int")))
          .withColumn("account_id", F.lpad(((F.col("rid") % 40)).cast("string"), 12, "0"))
          .withColumn("private_ip", F.concat(F.lit("10."),
                       ((F.col("rid")/65536).cast("int") % 256).cast("string"), F.lit("."),
                       ((F.col("rid")/256).cast("int") % 256).cast("string"), F.lit("."),
                       (F.col("rid") % 256).cast("string")))
          .withColumn("owner_email",
                       F.concat(F.lit("owner"), (F.col("rid") % 500).cast("string"), F.lit("@example-corp.internal")))
          .drop("rid"))
    return df

def gen_flows(spark, n, n_res, run_date):
    # VPC Flow Log v2 fields: version account-id interface-id srcaddr dstaddr
    # srcport dstport protocol packets bytes start end action log-status
    df = (spark.range(n).withColumnRenamed("id", "eid")
          .withColumn("version", F.lit(2))
          .withColumn("account_id", F.lpad(((F.col("eid") % 40)).cast("string"), 12, "0"))
          .withColumn("interface_id", F.concat(F.lit("eni-"), (F.col("eid") % n_res).cast("string")))
          .withColumn("src_rid", (F.col("eid") % n_res))
          .withColumn("dst_rid", ((F.col("eid") * F.lit(2654435761) % F.lit(n_res)).cast("long")))
          .withColumn("srcaddr", F.concat(F.lit("10."),
                       ((F.col("src_rid")/65536).cast("int") % 256).cast("string"), F.lit("."),
                       ((F.col("src_rid")/256).cast("int") % 256).cast("string"), F.lit("."),
                       (F.col("src_rid") % 256).cast("string")))
          .withColumn("dstaddr", F.concat(F.lit("10."),
                       ((F.col("dst_rid")/65536).cast("int") % 256).cast("string"), F.lit("."),
                       ((F.col("dst_rid")/256).cast("int") % 256).cast("string"), F.lit("."),
                       (F.col("dst_rid") % 256).cast("string")))
          .withColumn("srcport", (F.col("eid") % 60000 + 1024).cast("int"))
          .withColumn("dstport", F.element_at(F.array(F.lit(443),F.lit(3306),F.lit(5432),F.lit(6379),F.lit(80),F.lit(22)),
                       (F.col("eid") % 6 + 1).cast("int")))
          .withColumn("protocol", F.lit(6))
          .withColumn("packets", (F.col("eid") % 900 + 1).cast("long"))
          .withColumn("bytes", (F.col("eid") % 90000 + 40).cast("long"))
          .withColumn("start_ts", (F.lit(1754697600) + (F.col("eid") % 86400)).cast("long"))
          .withColumn("end_ts", (F.lit(1754697600) + (F.col("eid") % 86400) + 30).cast("long"))
          .withColumn("action", F.when(F.col("eid") % 50 == 0, F.lit("REJECT")).otherwise(F.lit("ACCEPT")))
          .withColumn("log_status", F.lit("OK"))
          .withColumn("dt", F.lit(run_date))
          .drop("src_rid", "dst_rid", "eid"))
    # Inject ~3% exact-duplicate rows (at-least-once delivery reality) to exercise dedup
    dupes = df.limit(int(n * 0.03))
    return df.unionByName(dupes)

def gen_api(spark, n, n_principals, n_res, run_date):
    df = (spark.range(n).withColumnRenamed("id", "eid")
          .withColumn("event_time", (F.lit(1754697600) + (F.col("eid") % 86400)).cast("long"))
          .withColumn("event_name", F.element_at(F.array(*[F.lit(a) for a in ACTIONS]),
                       (F.col("eid") % len(ACTIONS) + 1).cast("int")))
          .withColumn("principal_id", F.concat(F.lit("p-"), (F.col("eid") % n_principals).cast("string")))
          .withColumn("resource_id", F.concat(F.lit("r-"), (F.col("eid") % n_res).cast("string")))
          .withColumn("source_ip", F.concat(F.lit("52."),
                       ((F.col("eid")/256).cast("int") % 256).cast("string"), F.lit("."),
                       (F.col("eid") % 256).cast("string"), F.lit(".10")))
          .withColumn("error_code", F.when(F.col("eid") % 200 == 0, F.lit("AccessDenied")).otherwise(F.lit(None)))
          .withColumn("dt", F.lit(run_date))
          .drop("eid"))
    return df

def main():
    cfg = load_cfg()
    scale_key = sys.argv[1] if len(sys.argv) > 1 else "full"
    s = cfg["scale"]
    if scale_key == "sample":   # used by CI: tiny, fast, same code path
        s = {"n_resources": 500, "n_principals": 100, "n_flow_events": 20000, "n_api_events": 5000}
    spark = spark_session("gen", cfg); spark.sparkContext.setLogLevel("ERROR")
    rd = cfg["run_date"]; raw = cfg["paths"]["raw"]

    gen_resources(spark, s["n_resources"]).write.mode("overwrite").parquet(f"{raw}/resources")
    (gen_flows(spark, s["n_flow_events"], s["n_resources"], rd)
        .write.mode("overwrite").partitionBy("dt").parquet(f"{raw}/vpc_flow_logs"))
    (gen_api(spark, s["n_api_events"], s["n_principals"], s["n_resources"], rd)
        .write.mode("overwrite").partitionBy("dt").parquet(f"{raw}/cloudtrail"))
    print(f"[generate] wrote raw synthetic data at scale='{scale_key}'")
    spark.stop()

if __name__ == "__main__":
    main()
