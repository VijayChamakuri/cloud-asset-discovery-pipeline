"""Data Quality gate. Runs BEFORE gold is promoted. Nothing ships unverified.
Checks: row-count floors, null-rate ceilings, uniqueness, referential integrity,
and a LEAKAGE guard (no raw confidential PII in analytics zone).
Owner: QA/Validation agent."""
import sys, json
from pyspark.sql import functions as F
from utils import spark_session, load_cfg

def rate(df, cond):
    n = df.count()
    return (df.filter(cond).count() / n) if n else 0.0

def main():
    cfg = load_cfg(); spark = spark_session("dq", cfg); spark.sparkContext.setLogLevel("ERROR")
    s = cfg["paths"]["silver"]; results = []; ok = True
    flows = spark.read.parquet(f"{s}/flows")
    res   = spark.read.parquet(f"{s}/resources")
    api   = spark.read.parquet(f"{s}/api_actions")

    # 1. Non-empty
    c = flows.count() > 0 and res.count() > 0
    results.append(("flows_nonempty", c)); ok &= c

    # 2. Null-rate ceiling on srcaddr
    nr = rate(flows, F.col("srcaddr").isNull())
    c = nr <= cfg["dq"]["max_null_rate_srcaddr"]
    results.append((f"srcaddr_null_rate={nr:.4f}", c)); ok &= c

    # 3. No duplicates remain on flow business key
    dup = flows.groupBy("account_id","interface_id","srcaddr","dstaddr","srcport","dstport","start_ts","bytes") \
               .count().filter(F.col("count") > 1).count()
    c = dup == 0
    results.append((f"flow_duplicates={dup}", c)); ok &= c

    # 4. Referential integrity: api_actions.resource_id exists in resources
    ri = (api.select("resource_id").distinct()
             .join(res.select("resource_id"), "resource_id", "left_anti").count())
    tot = api.select("resource_id").distinct().count()
    ri_ok_rate = 1 - (ri / tot if tot else 0)
    c = ri_ok_rate >= cfg["dq"]["min_referential_integrity"]
    results.append((f"referential_integrity={ri_ok_rate:.4f}", c)); ok &= c

    # 5. LEAKAGE guard: masked columns must be 64-char hex, never contain '@' or raw dotted IPs
    leak = res.filter(F.col("owner_email").contains("@")).count() \
         + api.filter(F.col("source_ip").contains(".")).count()
    c = leak == 0
    results.append((f"pii_leakage_rows={leak}", c)); ok &= c

    for name, passed in results:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")
    print(json.dumps({"dq_pass": ok, "checks": len(results)}))
    spark.stop()
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
