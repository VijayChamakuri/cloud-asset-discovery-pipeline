"""Shared helpers: Spark session + config loading. Owner: Data Engineer agent."""
import os, yaml
from pyspark.sql import SparkSession

# Bind Spark to loopback so it does not depend on hostname/DNS resolution.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

def load_cfg(path="config/pipeline.yml"):
    with open(path) as f:
        return yaml.safe_load(f)

def spark_session(app="asset-discovery", cfg=None):
    # driver.memory is intentionally modest so the single local JVM stays within
    # container RAM; large stages spill to spark.local.dir on disk rather than OOM.
    b = (SparkSession.builder.appName(app).master("local[*]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEM", "2g"))
         .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIR", "/tmp/spark-tmp"))
         .config("spark.sql.parquet.compression.codec", "snappy"))
    if cfg:
        b = b.config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
    return b.getOrCreate()
