"""Bronze layer: land raw multi-source data, add ingest metadata, partition.
No business logic — faithful capture. Owner: Data Engineer agent."""
from pyspark.sql import functions as F
from utils import spark_session, load_cfg

def ingest(spark, src, dst, partition=None):
    df = spark.read.parquet(src).withColumn("_ingested_at", F.current_timestamp()) \
                                .withColumn("_source_file", F.input_file_name())
    w = df.write.mode("overwrite")
    if partition: w = w.partitionBy(partition)
    w.parquet(dst)
    return df.count()

def main():
    cfg = load_cfg(); spark = spark_session("bronze", cfg); spark.sparkContext.setLogLevel("ERROR")
    raw, bronze = cfg["paths"]["raw"], cfg["paths"]["bronze"]
    n_flows = ingest(spark, f"{raw}/vpc_flow_logs", f"{bronze}/vpc_flow_logs", "dt")
    n_api   = ingest(spark, f"{raw}/cloudtrail",    f"{bronze}/cloudtrail", "dt")
    n_res   = ingest(spark, f"{raw}/resources",     f"{bronze}/resources")
    print(f"[bronze] flows={n_flows} api={n_api} resources={n_res}")
    spark.stop()

if __name__ == "__main__":
    main()
