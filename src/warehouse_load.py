"""Load gold dimensional model into a DuckDB warehouse (local stand-in for
Redshift/Athena — labeled as such) and run the analytics SQL. Owner: Analytics agent."""
import duckdb, glob, os
from utils import load_cfg

def main():
    cfg = load_cfg(); gold = cfg["paths"]["gold"]; wh = cfg["paths"]["warehouse"]
    os.makedirs(os.path.dirname(wh), exist_ok=True)
    try:
        if os.path.exists(wh): os.remove(wh)
        con = duckdb.connect(wh)
    except Exception:
        # Some sandboxed/mounted filesystems disallow the duckdb file lock; fall back.
        wh = "/tmp/discovery.duckdb"
        if os.path.exists(wh): os.remove(wh)
        con = duckdb.connect(wh)
    print(f"[warehouse] connected: {wh}")
    for tbl in ["dim_resource","dim_principal","fact_network_flow","fact_api_action","resource_application_map"]:
        p = f"{gold}/{tbl}"
        if glob.glob(f"{p}/**/*.parquet", recursive=True):
            con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet('{p}/**/*.parquet')")
            n = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
            print(f"  loaded {tbl}: {n} rows")
    print("[warehouse] top talkers:")
    for row in con.execute("""
        SELECT dr.service, count(*) AS flows, sum(f.bytes) AS total_bytes
        FROM fact_network_flow f JOIN dim_resource dr ON f.srcaddr = dr.private_ip
        GROUP BY 1 ORDER BY total_bytes DESC LIMIT 5""").fetchall():
        print("   ", row)
    con.close()

if __name__ == "__main__":
    main()
