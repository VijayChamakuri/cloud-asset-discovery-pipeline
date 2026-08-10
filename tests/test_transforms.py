"""Unit tests for the transform logic — run in CI on a local Spark session.
Owner: QA/Validation agent."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import os
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
import pytest
from pyspark.sql import SparkSession, types as T
from silver_transform import transform_flows, transform_api, mask_col, CONFIDENTIAL

@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("test")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1").getOrCreate())
    s.sparkContext.setLogLevel("ERROR"); yield s; s.stop()

def test_dedup_removes_exact_duplicates(spark):
    schema = T.StructType([T.StructField(c, t, True) for c, t in [
        ("account_id",T.StringType()),("interface_id",T.StringType()),("srcaddr",T.StringType()),
        ("dstaddr",T.StringType()),("srcport",T.IntegerType()),("dstport",T.IntegerType()),
        ("packets",T.LongType()),("bytes",T.LongType()),
        ("start_ts",T.LongType()),("end_ts",T.LongType())]])
    rows = [("0"*12,"eni-1","10.0.0.1","10.0.0.2",1024,443,10,100,1000,1030)] * 3
    df = spark.createDataFrame(rows, schema)
    out, before, after = transform_flows(df)
    assert before == 3 and after == 1

def test_masking_is_irreversible_and_no_plaintext(spark):
    schema = T.StructType([T.StructField(c, t, True) for c, t in [
        ("principal_id",T.StringType()),("resource_id",T.StringType()),("event_name",T.StringType()),
        ("event_time",T.LongType()),("source_ip",T.StringType()),("error_code",T.StringType())]])
    df = spark.createDataFrame([("p-1","r-1","GetObject",1000,"52.1.2.3",None)], schema)
    out = transform_api(df).collect()[0]
    assert "." not in out["source_ip"]           # no dotted IP survives
    assert len(out["source_ip"]) == 64           # sha256 hex

def test_confidential_registry_nonempty():
    assert "owner_email" in CONFIDENTIAL and "source_ip" in CONFIDENTIAL
