-- Production target DDL (Amazon Redshift). Gold tables use distribution/sort keys
-- tuned for the analytics access patterns; an Apache Iceberg variant follows.
CREATE TABLE dim_resource (
  resource_id   VARCHAR(64) NOT NULL,
  service       VARCHAR(32),
  app_id        VARCHAR(32),
  account_id    CHAR(12),
  private_ip    VARCHAR(15),
  owner_email   CHAR(64)          -- tokenized (sha256), never plaintext PII
) DISTSTYLE KEY DISTKEY (resource_id) SORTKEY (account_id, service);

CREATE TABLE fact_network_flow (
  account_id  CHAR(12), interface_id VARCHAR(32),
  srcaddr VARCHAR(15), dstaddr VARCHAR(15),
  srcport INT, dstport INT, protocol INT,
  packets BIGINT, bytes BIGINT,
  start_ts TIMESTAMP, end_ts TIMESTAMP, action VARCHAR(8), dt DATE
) DISTSTYLE KEY DISTKEY (srcaddr) SORTKEY (dt, account_id);

-- Iceberg equivalent (EMR/Athena/Glue), partitioned by day for pruning:
-- CREATE TABLE gold.fact_network_flow (...) USING iceberg
--   PARTITIONED BY (days(start_ts))
--   TBLPROPERTIES ('format-version'='2','write.target-file-size-bytes'='134217728');
