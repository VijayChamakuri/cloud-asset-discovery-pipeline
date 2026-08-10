"""Silver layer: clean, type-cast, DEDUPLICATE, and apply DATA GOVERNANCE
(classify + mask confidential fields). Conform schemas for downstream modeling.
Owner: Data Engineer agent."""
import hashlib
from pyspark.sql import functions as F, types as T
from utils import spark_session, load_cfg

# Column-level data classification -> maps to "highly confidential data handling".
# CONFIDENTIAL fields are tokenized (sha256) so joins still work but raw PII never
# lands in the analytics zone. This is the core confidential-data governance control.
CONFIDENTIAL = {"owner_email", "source_ip"}

def mask_col(c):
    return F.sha2(F.col(c).cast("string"), 256)

def transform_flows(df):
    before = df.count()
    df = (df.withColumn("bytes", F.col("bytes").cast("long"))
            .withColumn("packets", F.col("packets").cast("long"))
            .withColumn("start_ts", F.to_timestamp(F.from_unixtime("start_ts")))
            .withColumn("end_ts", F.to_timestamp(F.from_unixtime("end_ts")))
            .filter(F.col("srcaddr").isNotNull() & F.col("dstaddr").isNotNull())
            .dropDuplicates(["account_id","interface_id","srcaddr","dstaddr",
                             "srcport","dstport","start_ts","bytes"]))
    after = df.count()
    return df, before, after

def transform_api(df):
    return (df.withColumn("event_time", F.to_timestamp(F.from_unixtime("event_time")))
              .withColumn("source_ip", mask_col("source_ip"))   # governance: mask
              .dropDuplicates(["principal_id","resource_id","event_name","event_time"]))

def transform_resources(df):
    return df.withColumn("owner_email", mask_col("owner_email"))  # governance: mask

def main():
    cfg = load_cfg(); spark = spark_session("silver", cfg); spark.sparkContext.setLogLevel("ERROR")
    b, s = cfg["paths"]["bronze"], cfg["paths"]["silver"]
    flows, n_before, n_after = transform_flows(spark.read.parquet(f"{b}/vpc_flow_logs"))
    flows.write.mode("overwrite").partitionBy("dt").parquet(f"{s}/flows")
    transform_api(spark.read.parquet(f"{b}/cloudtrail")).write.mode("overwrite").partitionBy("dt").parquet(f"{s}/api_actions")
    transform_resources(spark.read.parquet(f"{b}/resources")).write.mode("overwrite").parquet(f"{s}/resources")
    dedup_rate = 100.0 * (n_before - n_after) / n_before if n_before else 0
    print(f"[silver] flows_in={n_before} flows_out={n_after} dedup_removed_pct={dedup_rate:.2f}")
    spark.stop()

if __name__ == "__main__":
    main()
