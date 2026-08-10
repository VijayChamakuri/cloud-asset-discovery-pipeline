# Architecture & Multi-Agent Delivery

## Data flow
```
                 ┌─────────── Airflow DAG: asset_discovery (daily 03:00) ───────────┐
                 │                                                                   │
 synthetic -> generate -> bronze -> silver -> [DQ GATE] -> gold -> spark-sql -> warehouse -> dbt -> notify
 sources        (raw)      (land)   (clean/    (5 checks,   (dims+  (HiveQL      (DuckDB/   (marts  (SNS->
 (VPC flow,                         dedup/     stop on      app     window       Redshift   +12     SQS,
  CloudTrail,                       mask)      fail)        disc)   funcs)       stand-in)  tests)  boto3)
  inventory)
                 │                                                                   │
                 └── partitioned Parquet on local FS (S3 stand-in), dt= partitions ──┘
```

## AWS mapping (what each local piece stands in for)
| This repo | Production AWS equivalent |
|---|---|
| PySpark local (incl. HiveQL Spark SQL) | EMR / Glue Spark (HiveQL runs unchanged) |
| Partitioned Parquet on disk | S3 + Glue Catalog |
| DuckDB warehouse + dbt-duckdb marts | Redshift / Athena + dbt |
| `src/notify.py` (boto3 SNS→SQS, moto-tested) | SNS publish / SQS delivery |
| `sql/redshift_ddl.sql` | Redshift DDL + Apache Iceberg tables |
| Airflow DAG | MWAA (Managed Airflow) |

## Data governance
`silver_transform.py` holds a `CONFIDENTIAL` registry (`owner_email`,
`source_ip`). Those columns are tokenized with SHA-256 before data enters the
analytics zone, so joins still work but raw PII never lands downstream. The DQ
gate has a dedicated **leakage guard** that fails the run if any plaintext PII is
detected in silver. This is the "highly confidential data handling" control the
role calls for.

## Multi-agent delivery model (role assignment)
The build was split across specialized roles, run in parallel where independent
and integrated at the DQ gate:

| Agent role | Owns | Artifacts |
|---|---|---|
| **Data Engineer** | ingestion, cleaning, dedup, governance, orchestration | `generate`, `bronze`, `silver`, Airflow DAG |
| **Analytics / Modeling** | dimensional model + application discovery | `gold_model.py`, `warehouse_load.py`, `analytics_queries.sql` |
| **QA / Validation** | data-quality gate + independent re-verification (2nd engine), leakage guard, unit tests | `dq_checks.py`, `tests/`, `docs/RESULTS.md` |
| **Reviewer / PM** | scope control, requirement alignment, legibility | this doc, README structure, honesty labeling |
| **Docs** | README, results, plain-English summary | `README.md`, `docs/EXECUTIVE_SUMMARY.md` |

Coordination: Data Engineer and Analytics built their stages against a shared
contract (the silver schema); QA built the gate and tests concurrently against the
same contract; the DQ gate is the integration point where nothing proceeds to gold
unless QA's checks pass; Reviewer/PM and Docs closed out.

Provenance is labeled honestly throughout: synthetic data on public cloud-log
schemas, never implied to be real production data.
