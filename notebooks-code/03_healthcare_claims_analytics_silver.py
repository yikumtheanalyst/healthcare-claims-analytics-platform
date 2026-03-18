# Databricks notebook source
# MAGIC %md
# MAGIC # Project Type: Enterprise-Style Synthetic Healthcare Claims Data Platform

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 — Silver Layer: healthcare Claims Standardization (Clean Template)
# MAGIC
# MAGIC **Purpose:** Transform Bronze raw tables into **clean, typed, validated** Silver entity tables (no business joins).  
# MAGIC **Architecture:** Medallion (Bronze → Silver → Gold)
# MAGIC
# MAGIC ### Silver Principles Applied
# MAGIC - Read from Bronze (raw truth) and process **one run/batch** (run_id) deterministically
# MAGIC - Standardize types, formats, and keys (trim/upper)
# MAGIC - Apply lightweight data quality gates (required keys, castability)
# MAGIC - Write clean entity tables + rejects tables
# MAGIC - Structured run logging (rows_read / rows_written / rows_rejected are **run-level** metrics)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Imports

# COMMAND ----------

from datetime import datetime
import uuid
import re

from pyspark.sql import functions as F
from pyspark.sql import types as T


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Parameters (Widgets)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# Bronze source
dbutils.widgets.text("source_catalog", "bronze_analytics")        # catalog
dbutils.widgets.text("source_schema", "claims_analytics")     # schema

# Silver destination
dbutils.widgets.text("destination_catalog", "silver_analytics")   # catalog
dbutils.widgets.text("destination_schema", "claims_analytics")# schema

# Which Bronze batch to process
# - "latest" = find the most recent ingestion_timestamp and process only that run_id
# - or provide an explicit run_id UUID
dbutils.widgets.text("process_run_id", "latest")

# Write mode for Silver (overwrite is typical for entity tables)
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Derived Variables + Run Context

# COMMAND ----------

run_id = str(uuid.uuid4())
start_time = datetime.now()

src_catalog = dbutils.widgets.get("source_catalog")
src_schema  = dbutils.widgets.get("source_schema")

dst_catalog = dbutils.widgets.get("destination_catalog")
dst_schema  = dbutils.widgets.get("destination_schema")

process_run_id = dbutils.widgets.get("process_run_id").strip().lower()
write_mode = dbutils.widgets.get("write_mode").strip().lower()

# Bronze tables
bronze_member_table         = f"{src_catalog}.{src_schema}.members"
bronze_membership_month_table = f"{src_catalog}.{src_schema}.membership_month" 
bronze_claims_header        = f"{src_catalog}.{src_schema}.claims_header"
bronze_claims_diag_table    = f"{src_catalog}.{src_schema}.claims_diagnosis"
bronze_claims_lines_table   = f"{src_catalog}.{src_schema}.claims_lines"

bronze_icd10_table      = f"{src_catalog}.{src_schema}.ref_icd10_diagnosis"
bronze_hcpcs_table      = f"{src_catalog}.{src_schema}.ref_hcpcs"
bronze_rev_table        = f"{src_catalog}.{src_schema}.ref_revenue_codes"
bronze_pos_table        = f"{src_catalog}.{src_schema}.ref_place_of_service"
bronze_providers_table  = f"{src_catalog}.{src_schema}.providers"
bronze_payers_table     = f"{src_catalog}.{src_schema}.payers"


# Silver tables (clean entities)
silver_claims_table      = f"{dst_catalog}.{dst_schema}.claims_clean"
silver_claims_rejects    = f"{dst_catalog}.{dst_schema}.claims_rejects"

silver_icd10_table       = f"{dst_catalog}.{dst_schema}.ref_icd10_diagnosis"
silver_hcpcs_table       = f"{dst_catalog}.{dst_schema}.ref_hcpcs"
silver_rev_table         = f"{dst_catalog}.{dst_schema}.ref_revenue_codes"
silver_pos_table         = f"{dst_catalog}.{dst_schema}.ref_place_of_service"

silver_members_table            = f"{dst_catalog}.{dst_schema}.members"
silver_membership_month_table            = f"{dst_catalog}.{dst_schema}.membership_month"

silver_claims_header_table      = f"{dst_catalog}.{dst_schema}.claims_header"
silver_claims_diagnosis_table   = f"{dst_catalog}.{dst_schema}.claims_diagnosis"
silver_claims_lines_table       = f"{dst_catalog}.{dst_schema}.claims_lines"
silver_provider_table           = f"{dst_catalog}.{dst_schema}.providers"
silver_payers_table             = f"{dst_catalog}.{dst_schema}.payers"


# Silver run log
run_log_table            = f"{dst_catalog}.{dst_schema}.pipeline_run_log"

print(f"silver_run_id: {run_id}")
print(f"process_run_id: {process_run_id}")
print(f"write_mode: {write_mode}")
print(f"bronze_member_table: {bronze_member_table}")
print(f"bronze_claims_header: {bronze_claims_header}")
print(f"bronze_claims_diag_table: {bronze_claims_diag_table}")
print(f"bronze_claims_lines_table: {bronze_claims_lines_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Create Silver Run Log Table (if not exists)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {run_log_table} (
  run_id STRING,
  pipeline_name STRING,
  layer STRING,
  source STRING,
  target STRING,
  status STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  rows_read LONG,
  rows_written LONG,
  rows_rejected LONG,
  error_message STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Helper Functions

# COMMAND ----------

def log_run(
    pipeline_name: str,
    layer: str,
    source: str,
    target: str,
    status: str,
    start_time_dt: datetime,
    rows_read: int,
    rows_written: int,
    rows_rejected: int,
    error_message: str = None
):
    end_time_dt = datetime.now()
    duration_seconds = (end_time_dt - start_time_dt).total_seconds()

    log_df = spark.createDataFrame([(
        run_id,
        pipeline_name,
        layer,
        source,
        target,
        status,
        start_time_dt,
        end_time_dt,
        float(duration_seconds),
        int(rows_read) if rows_read is not None else None,
        int(rows_written) if rows_written is not None else None,
        int(rows_rejected) if rows_rejected is not None else None,
        error_message
    )], schema="""
        run_id string, pipeline_name string, layer string, source string, target string,
        status string, start_time timestamp, end_time timestamp, duration_seconds double,
        rows_read long, rows_written long, rows_rejected long, error_message string
    """)

    (log_df.write
          .format("delta")
          .option("overwriteSchema", "true")
          .option("mergeSchema", "true")
          .mode("append")
          .saveAsTable(run_log_table))


def write_delta(df, target_table: str, mode: str = "overwrite", partition_cols: list[str] | None = None) -> int:
    """Write a dataframe to a Delta table and return rows written for THIS run.

    Notes:
    - We cache to avoid recomputing df for both count() and write().
    - overwriteSchema is only applied when mode == 'overwrite'.
    - partitioning is optional (pass partition_cols=['date_key'] etc.).
    """
    # If you're using Free Databricks Community Edition, you'll get the following error:
    #[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute. SQLSTATE: 0A000

    # df_cached = df.cache()
    # rows_written = df_cached.count()
      # In production, the above settings can be used, by removing the following line
    rows_written = df.count()

    # writer = (df_cached.write  # In production, this can be uncommented and used, by removing the following line
    writer = (df.write
              .format("delta")
              .mode(mode)
              .option("mergeSchema", "true"))

    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(target_table)

    # df_cached.unpersist() # Uncomment in production
    return int(rows_written)


def clean_string_cols(df):
    """Trim all string columns; keep original column names."""
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


def upper_if_exists(df, col_name: str):
    if col_name in df.columns:
        return df.withColumn(col_name, F.upper(F.col(col_name)))
    return df


def cast_if_exists(df, col_name: str, spark_type: str):
    """Safely cast a column if it exists. Uses try_cast where available."""
    if col_name not in df.columns:
        return df
    # try_cast is safest; fallback to cast if not supported
    try:
        return df.withColumn(col_name, F.expr(f"try_cast({col_name} as {spark_type})"))
    except Exception:
        return df.withColumn(col_name, F.col(col_name).cast(spark_type))


def to_date_multi(df, col_name: str):
    """Parse dates from common formats; leaves null if not parseable."""
    if col_name not in df.columns:
        return df
    return df.withColumn(
        col_name,
        F.coalesce(
            F.to_date(F.col(col_name), "yyyy-MM-dd"),
            F.to_date(F.col(col_name), "MM/dd/yyyy"),
            F.to_date(F.col(col_name), "M/d/yyyy"),
            F.to_date(F.col(col_name), "dd-MM-yyyy"),
            F.to_date(F.col(col_name), "dd/MM/yyyy")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Determine the Bronze run_id to Process

# COMMAND ----------

if not spark.catalog.tableExists(bronze_member_table):
    raise ValueError(f"Bronze claims table not found: {bronze_member_table}")


df_members = spark.table(bronze_member_table)

# df_members = spark.table(bronze_member_table).dropDuplicates(["member_id"])

if "run_id" not in df_members.columns or "ingestion_timestamp" not in df_members.columns:
    raise ValueError("Bronze claims table must contain run_id and ingestion_timestamp columns.")

if process_run_id == "latest":
    latest_row = (df_members
                  .orderBy(F.col("ingestion_timestamp").desc())
                  .select("run_id", "ingestion_timestamp")
                  .limit(1)
                  .collect())
    if not latest_row:
        raise ValueError("Bronze claims table is empty.")
    bronze_run_id = latest_row[0]["run_id"]
    bronze_ingestion_ts = latest_row[0]["ingestion_timestamp"]
else:
    bronze_run_id = dbutils.widgets.get("process_run_id").strip()
    bronze_ingestion_ts = None

print(f"Processing bronze_run_id: {bronze_run_id}")
if bronze_ingestion_ts is not None:
    print(f"Latest ingestion_timestamp: {bronze_ingestion_ts}")

# Memberhip Month
# bronze_membership_month_table
if not spark.catalog.tableExists(bronze_membership_month_table):
    raise ValueError(f"Bronze claims table not found: {bronze_membership_month_table}")


df_membershipt_month = spark.table(bronze_membership_month_table)

# df_membershipt_month = spark.table(bronze_membership_month_table).dropDuplicates(["member_id"])

if "run_id" not in df_membershipt_month.columns or "ingestion_timestamp" not in df_membershipt_month.columns:
    raise ValueError("Bronze claims table must contain run_id and ingestion_timestamp columns.")

if process_run_id == "latest":
    latest_row = (df_membershipt_month
                  .orderBy(F.col("ingestion_timestamp").desc())
                  .select("run_id", "ingestion_timestamp")
                  .limit(1)
                  .collect())
    if not latest_row:
        raise ValueError("Bronze claims table is empty.")
    bronze_run_id = latest_row[0]["run_id"]
    bronze_ingestion_ts = latest_row[0]["ingestion_timestamp"]
else:
    bronze_run_id = dbutils.widgets.get("process_run_id").strip()
    bronze_ingestion_ts = None

print(f"Processing bronze_run_id: {bronze_run_id}")
if bronze_ingestion_ts is not None:
    print(f"Latest ingestion_timestamp: {bronze_ingestion_ts}")

# Claims Header
if not spark.catalog.tableExists(bronze_claims_header):
    raise ValueError(f"Bronze claims table not found: {bronze_claims_header}")

df_claims_header = spark.table(bronze_claims_header)

if "run_id" not in df_claims_header.columns or "ingestion_timestamp" not in df_claims_header.columns:
    raise ValueError("Bronze claims table must contain run_id and ingestion_timestamp columns.")

if process_run_id == "latest":
    latest_row = (df_claims_header
                  .orderBy(F.col("ingestion_timestamp").desc())
                  .select("run_id", "ingestion_timestamp")
                  .limit(1)
                  .collect())
    if not latest_row:
        raise ValueError("Bronze claims table is empty.")
    bronze_run_id = latest_row[0]["run_id"]
    bronze_ingestion_ts = latest_row[0]["ingestion_timestamp"]
else:
    bronze_run_id = dbutils.widgets.get("process_run_id").strip()
    bronze_ingestion_ts = None

print(f"Processing bronze_run_id: {bronze_run_id}")
if bronze_ingestion_ts is not None:
    print(f"Latest ingestion_timestamp: {bronze_ingestion_ts}")

# Claims Diagnosis
if not spark.catalog.tableExists(bronze_claims_diag_table):
    raise ValueError(f"Bronze claims table not found: {bronze_claims_diag_table}")

df_claims_diagnosis = spark.table(bronze_claims_diag_table)

if "run_id" not in df_claims_diagnosis.columns or "ingestion_timestamp" not in df_claims_diagnosis.columns:
    raise ValueError("Bronze claims table must contain run_id and ingestion_timestamp columns.")

if process_run_id == "latest":
    latest_row = (df_claims_diagnosis
                  .orderBy(F.col("ingestion_timestamp").desc())
                  .select("run_id", "ingestion_timestamp")
                  .limit(1)
                  .collect())
    if not latest_row:
        raise ValueError("Bronze claims table is empty.")
    bronze_run_id = latest_row[0]["run_id"]
    bronze_ingestion_ts = latest_row[0]["ingestion_timestamp"]
else:
    bronze_run_id = dbutils.widgets.get("process_run_id").strip()
    bronze_ingestion_ts = None

print(f"Processing bronze_run_id: {bronze_run_id}")
if bronze_ingestion_ts is not None:
    print(f"Latest ingestion_timestamp: {bronze_ingestion_ts}")

# Claims Lines
if not spark.catalog.tableExists(bronze_claims_lines_table):
    raise ValueError(f"Bronze claims table not found: {bronze_claims_lines_table}")

df_claims_lines = spark.table(bronze_claims_lines_table)

if "run_id" not in df_claims_lines.columns or "ingestion_timestamp" not in df_claims_lines.columns:
    raise ValueError("Bronze claims table must contain run_id and ingestion_timestamp columns.")

if process_run_id == "latest":
    latest_row = (df_claims_lines
                  .orderBy(F.col("ingestion_timestamp").desc())
                  .select("run_id", "ingestion_timestamp")
                  .limit(1)
                  .collect())
    if not latest_row:
        raise ValueError("Bronze claims table is empty.")
    bronze_run_id = latest_row[0]["run_id"]
    bronze_ingestion_ts = latest_row[0]["ingestion_timestamp"]
else:
    bronze_run_id = dbutils.widgets.get("process_run_id").strip()
    bronze_ingestion_ts = None

print(f"Processing bronze_run_id: {bronze_run_id}")
if bronze_ingestion_ts is not None:
    print(f"Latest ingestion_timestamp: {bronze_ingestion_ts}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Build Silver: Claims Clean + Rejects (No joins)

# COMMAND ----------

pipeline_name = "claims_analytics_Pipeline"
layer = "silver"

rows_read = 0
rows_written = 0
rows_rejected = 0

silver_start = datetime.now()

try:
    df_claims_bronze = df_claims_lines.filter(F.col("run_id") == bronze_run_id)
    rows_read = df_claims_bronze.count()

    if rows_read == 0:
        raise ValueError(f"No rows found for bronze_run_id={bronze_run_id} in {bronze_claims_lines_table}")

    # 7.1 Minimal normalization (no joins)
    df = clean_string_cols(df_claims_bronze)

    # Common code columns (safe if missing)
    for code_col in ["diagnosis_code", "procedure_code", "revenue_code", "place_of_service_code", "ndc_code"]:
        df = upper_if_exists(df, code_col)

    # 7.2 Type standardization (only if columns exist)
    # Numeric money/amount fields
    numeric_cols = [
        "line_charge", "line_allowed", "line_paid", "paid_amount",
        "billed_amount", "allowed_amount", "member_cost_share"
    ]
    for c in numeric_cols:
        df = cast_if_exists(df, c, "decimal(18,2)")

    # Integer-like fields
    for c in ["units", "quantity", "days_supply"]:
        df = cast_if_exists(df, c, "int")

    # Date fields (common set; safe if missing)
    date_cols = ["claim_date", "service_date", "admission_date", "discharge_date", "fill_date", "paid_date"]
    for c in date_cols:
        df = to_date_multi(df, c)

    # 7.3 Required key checks (update your dataset with list of required keys)
    required_keys = [c for c in ["claim_id", "member_id"] if c in df.columns]
    if not required_keys:        
        print("WARNING: No required_keys found among ['claim_id','member_id']. Update required_keys if needed.")

    # Reject condition: any required key is null/blank
    reject_cond = None
    for k in required_keys:
        cnd = (F.col(k).isNull()) | (F.length(F.trim(F.col(k))) == 0)
        reject_cond = cnd if reject_cond is None else (reject_cond | cnd)

    if reject_cond is None:
        df_rejects = df.limit(0)  # empty
        df_clean = df
    else:
        df_rejects = df.filter(reject_cond).withColumn("reject_reason", F.lit("MISSING_REQUIRED_KEY"))
        df_clean   = df.filter(~reject_cond)

    rows_rejected = df_rejects.count()

    # 7.4 Add Silver metadata
    df_clean = (df_clean
                .withColumn("silver_run_id", F.lit(run_id))
                .withColumn("bronze_run_id", F.lit(bronze_run_id))
                .withColumn("silver_processed_timestamp", F.current_timestamp())
               )

    df_rejects = (df_rejects
                  .withColumn("silver_run_id", F.lit(run_id))
                  .withColumn("bronze_run_id", F.lit(bronze_run_id))
                  .withColumn("silver_processed_timestamp", F.current_timestamp())
                 )

    # 7.5 Write outputs (Silver typically overwrite for clean entity tables)
    rows_written = write_delta(df_clean, silver_claims_table, mode=write_mode)
    _ = write_delta(df_rejects, silver_claims_rejects, mode="append")  # keep all rejects across runs

    log_run(pipeline_name, layer, f"{bronze_claims_lines_table} (run_id={bronze_run_id})", silver_claims_table,
            "SUCCESS", silver_start, rows_read, rows_written, rows_rejected)

    print(f"Claims Silver complete. rows_read={rows_read}, rows_written={rows_written}, rows_rejected={rows_rejected}")

except Exception as e:
    log_run(pipeline_name, layer, f"{bronze_claims_lines_table} (run_id={bronze_run_id})", silver_claims_table,
            "FAILED", silver_start, rows_read, rows_written, rows_rejected, str(e)[:4000])
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Build Silver: Reference Tables (clean & overwrite)
# MAGIC Reference tables are usually small and can be overwritten each run.

# COMMAND ----------

def copy_ref_table(bronze_table: str, silver_table: str, ref_name: str):
    ref_start = datetime.now()
    rr = 0
    rw = 0
    rrej = 0
    try:
        if not spark.catalog.tableExists(bronze_table):
            raise ValueError(f"Bronze reference table not found: {bronze_table}")

        df_ref = spark.table(bronze_table)

        if str(ref_name).lower() == str("Members").lower():
            # Add Member age
            df_ref = df_ref.withColumn("age", F.abs(F.datediff(F.current_date(), F.to_date(F.col("birth_date"), "yyyy-MM-dd")) / 365.25)) 
            # Drop duplicate member ids
            # df_ref = df_ref.dropDuplicates(["member_id"])
            # df_ref = df_ref.drop("year")
        else:
            pass
        rr = df_ref.count()

        # Minimal normalization in Silver
        df_ref = clean_string_cols(df_ref)

        # Add metadata
        df_ref = (df_ref
                  .withColumn("silver_run_id", F.lit(run_id))
                  .withColumn("bronze_run_id", F.lit(bronze_run_id))
                  .withColumn("silver_processed_timestamp", F.current_timestamp())
                 )

        rw = write_delta(df_ref, silver_table, mode="overwrite")

        log_run(pipeline_name, layer, bronze_table, silver_table, "SUCCESS", ref_start, rr, rw, rrej)
        print(f"{ref_name} copied. rows_read={rr}, rows_written={rw}")
    except Exception as e:
        log_run(pipeline_name, layer, bronze_table, silver_table, "FAILED", ref_start, rr, rw, rrej, str(e)[:4000])
        raise

copy_ref_table(bronze_icd10_table, silver_icd10_table, "ICD10")
copy_ref_table(bronze_hcpcs_table, silver_hcpcs_table, "HCPCS")
copy_ref_table(bronze_rev_table,  silver_rev_table,  "Revenue Codes")
copy_ref_table(bronze_pos_table,  silver_pos_table,  "Place of Service")
copy_ref_table(bronze_member_table, silver_members_table, "Members")
copy_ref_table(bronze_membership_month_table, silver_membership_month_table, "Membership Month")

copy_ref_table(bronze_claims_header, silver_claims_header_table, "claims_header")
copy_ref_table(bronze_claims_diag_table, silver_claims_diagnosis_table, "claims_diagnosis")
copy_ref_table(bronze_claims_lines_table, silver_claims_lines_table, "claims_lines")
copy_ref_table(bronze_providers_table, silver_provider_table, "Providers")
copy_ref_table(bronze_payers_table, silver_payers_table, "Payers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9) Basic Validation (No EDA)
# MAGIC Confirm Silver tables exist and show run-level counts.

# COMMAND ----------

tables = [
    silver_claims_table,
    silver_claims_rejects,
    silver_icd10_table,
    silver_hcpcs_table,
    silver_rev_table,
    silver_pos_table, 
    silver_members_table,
    silver_claims_header_table,
    silver_claims_diagnosis_table,
    silver_claims_lines_table,
    silver_provider_table,
    silver_payers_table,
    run_log_table
]

for t in tables:
    exists = spark.catalog.tableExists(t)
    cnt = spark.table(t).count() if exists else 0
    print(f"{t}: exists={exists}, rows={cnt}")

# Per-run counts for this processing run
print("\nPer-run (this silver_run_id) counts:")
if spark.catalog.tableExists(silver_claims_table) and "silver_run_id" in spark.table(silver_claims_table).columns:
    (spark.table(silver_claims_table)
          .filter(F.col("silver_run_id") == run_id)
          .count())
print(f"silver_claims_written_this_run = {rows_written}")
print(f"silver_claims_rejected_this_run = {rows_rejected}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10) Review Latest Silver Run Logs

# COMMAND ----------

(spark.table(run_log_table)
      .filter(F.col("layer") == "silver")
      .orderBy(F.col("end_time").desc())
      .limit(50)
      .display())