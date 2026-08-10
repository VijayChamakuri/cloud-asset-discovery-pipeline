"""Gold layer: dimensional model + APPLICATION DISCOVERY.

Builds dim_resource, dim_principal, fact_network_flow, fact_api_action, and
discovers 'applications' by clustering resources that talk to each other on the
network (connected components over the resource-to-resource flow graph). Because
the synthetic resources carry a ground-truth app label, we also score the
discovered clusters against that label (homogeneity / completeness / V-measure
and purity) so the discovery quality is measured, not just counted.
Owner: Analytics/Modeling agent."""
import json
import math
from collections import defaultdict
from pyspark.sql import functions as F
from utils import spark_session, load_cfg


def cluster_metrics(pairs):
    """Entropy-based homogeneity / completeness / V-measure + purity.

    pairs: iterable of (predicted_cluster, true_class). Pure-Python (no sklearn)
    so the pipeline stays dependency-light. Definitions match scikit-learn:
      homogeneity  = 1 - H(K|C)/H(K)
      completeness = 1 - H(C|K)/H(C)
      V-measure    = 2*h*c/(h+c)
      purity       = sum_c max_k n_ck / N  (fraction of resources sharing their
                     cluster's majority true app == '% correctly grouped')
    """
    N = 0
    n_ck = defaultdict(lambda: defaultdict(int))  # cluster -> class -> count
    a_c = defaultdict(int)                         # cluster size
    a_k = defaultdict(int)                         # class size
    for c, k in pairs:
        n_ck[c][k] += 1; a_c[c] += 1; a_k[k] += 1; N += 1
    if N == 0:
        return {"n": 0}

    def entropy(counts):
        return -sum((v / N) * math.log(v / N) for v in counts if v > 0)

    H_C = entropy(a_c.values())
    H_K = entropy(a_k.values())
    # H(K|C) and H(C|K)
    H_K_given_C = -sum((cnt / N) * math.log(cnt / a_c[c])
                       for c in n_ck for cnt in n_ck[c].values() if cnt > 0)
    H_C_given_K = 0.0
    n_kc = defaultdict(dict)
    for c in n_ck:
        for k, cnt in n_ck[c].items():
            n_kc[k][c] = cnt
    H_C_given_K = -sum((cnt / N) * math.log(cnt / a_k[k])
                       for k in n_kc for cnt in n_kc[k].values() if cnt > 0)

    homogeneity = 1.0 if H_K == 0 else 1 - H_K_given_C / H_K
    completeness = 1.0 if H_C == 0 else 1 - H_C_given_K / H_C
    v = 0.0 if (homogeneity + completeness) == 0 else \
        2 * homogeneity * completeness / (homogeneity + completeness)
    purity = sum(max(n_ck[c].values()) for c in n_ck) / N
    return {
        "n_resources": N,
        "n_clusters": len(a_c),
        "n_true_apps": len(a_k),
        "homogeneity": round(homogeneity, 4),
        "completeness": round(completeness, 4),
        "v_measure": round(v, 4),
        "purity": round(purity, 4),
    }

def build_dims(spark, silver, gold):
    res = spark.read.parquet(f"{silver}/resources")
    res.write.mode("overwrite").parquet(f"{gold}/dim_resource")
    api = spark.read.parquet(f"{silver}/api_actions")
    (api.select("principal_id").distinct()
        .withColumn("principal_type",
            F.when(F.col("principal_id").startswith("p-"), F.lit("iam_principal")).otherwise(F.lit("unknown")))
     ).write.mode("overwrite").parquet(f"{gold}/dim_principal")
    api.write.mode("overwrite").partitionBy("dt").parquet(f"{gold}/fact_api_action")

def build_flow_facts(spark, silver, gold):
    flows = spark.read.parquet(f"{silver}/flows")
    flows.write.mode("overwrite").partitionBy("dt").parquet(f"{gold}/fact_network_flow")
    return flows

def discover_applications(spark, flows, res, gold):
    # Map IPs -> resource_id, build distinct resource-to-resource edges (small vs raw flows)
    ip2r = res.select(F.col("private_ip").alias("ip"), F.col("resource_id"))
    edges = (flows.filter(F.col("action") == "ACCEPT")
                  .join(ip2r.withColumnRenamed("ip","srcaddr").withColumnRenamed("resource_id","src_r"), "srcaddr")
                  .join(ip2r.withColumnRenamed("ip","dstaddr").withColumnRenamed("resource_id","dst_r"), "dstaddr")
                  .select("src_r","dst_r").filter(F.col("src_r") != F.col("dst_r")).distinct())
    # Union-Find on the driver (edge set is bounded by #resources, not #flows)
    pairs = [(r["src_r"], r["dst_r"]) for r in edges.collect()]
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb: parent[ra] = rb
    for a, b in pairs: union(a, b)
    all_r = [row["resource_id"] for row in res.select("resource_id").collect()]
    comp = [(r, f"discovered-app-{find(r)}") for r in all_r]
    ddf = spark.createDataFrame(comp, ["resource_id", "discovered_app_id"])
    joined = ddf.join(res.select("resource_id", F.col("app_id").alias("ground_truth_app")), "resource_id")
    joined.write.mode("overwrite").parquet(f"{gold}/resource_application_map")
    n_apps = joined.select("discovered_app_id").distinct().count()
    n_edges = len(pairs)
    # Score discovered clusters vs the ground-truth app label.
    label_pairs = [(r["discovered_app_id"], r["ground_truth_app"])
                   for r in joined.select("discovered_app_id", "ground_truth_app").collect()]
    metrics = cluster_metrics(label_pairs)
    with open(f"{gold}/discovery_accuracy.json", "w") as f:
        json.dump(metrics, f, indent=2)
    return n_apps, n_edges, metrics

def main():
    cfg = load_cfg(); spark = spark_session("gold", cfg); spark.sparkContext.setLogLevel("ERROR")
    silver, gold = cfg["paths"]["silver"], cfg["paths"]["gold"]
    build_dims(spark, silver, gold)
    flows = build_flow_facts(spark, silver, gold)
    res = spark.read.parquet(f"{silver}/resources")
    n_apps, n_edges, metrics = discover_applications(spark, flows, res, gold)
    print(f"[gold] discovered_applications={n_apps} distinct_edges={n_edges}")
    print(f"[gold] discovery_accuracy: homogeneity={metrics['homogeneity']} "
          f"completeness={metrics['completeness']} v_measure={metrics['v_measure']} "
          f"purity={metrics['purity']} (clusters={metrics['n_clusters']}, "
          f"true_apps={metrics['n_true_apps']})")
    spark.stop()

if __name__ == "__main__":
    main()
