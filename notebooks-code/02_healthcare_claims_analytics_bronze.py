# Databricks notebook source
# MAGIC %md
# MAGIC # Project Type: Enterprise-Style Synthetic Healthcare Claims Data Platform

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 — Bronze Layer: healthcare Claims Ingestion (Clean Template)
# MAGIC
# MAGIC **Author:** Yikum Shiferaw  
# MAGIC **Purpose:** Bronze (raw) ingestion for synthetic healthcare claims + reference code sets  
# MAGIC **Architecture:** Medallion (Bronze → Silver → Gold)
# MAGIC
# MAGIC ### Bronze Principles Applied
# MAGIC - Preserve raw truth (no business transformations, joins, or aggregations)
# MAGIC - Append-only loads (supports replay/auditing)
# MAGIC - Add ingestion metadata (run_id, ingestion_timestamp, source identifier)
# MAGIC - Minimal validations + structured run logging
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Imports

# COMMAND ----------

from datetime import datetime
import uuid
import pandas as pd

from pyspark.sql import functions as F

try:
    import openpyxl
    import xlrd 
    
except ImportError:
    %pip install openpyxl;
    %pip install xlrd;

run_id = str(uuid.uuid4())
start_time = datetime.now()    

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Parameters (Widgets)

# COMMAND ----------

# Clear widgets (safe for interactive use; harmless in jobs)
dbutils.widgets.removeAll()

# COMMAND ----------

# Source (Unity Catalog table)
dbutils.widgets.text("source_catalog", "bronze_analytics")       # catalog
dbutils.widgets.text("source_schema", "claims_analytics")  # schema

# Destination (your Bronze schema)
dbutils.widgets.text("destination_catalog", "bronze_analytics")  # catalog
dbutils.widgets.text("destination_schema", "claims_analytics") # schema

# Optional: reference files location (Unity Catalog Volumes)
dbutils.widgets.text("reference_volume_path", "/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/raw/")

# Optional: write mode (append recommended for Bronze)
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite"])

# Optional: validate table existence + counts (counts can be expensive)
dbutils.widgets.dropdown("validate_counts", "false", ["false", "true"])

source_path = dbutils.widgets.get("reference_volume_path")
source_path


dest_catalog = dbutils.widgets.get("destination_catalog")
dest_schema  = dbutils.widgets.get("destination_schema")

# Bronze target tables (separated by domain)
icd10_bronze_table       = f"{dest_catalog}.{dest_schema}.ref_icd10_diagnosis"
hcpcs_bronze_table       = f"{dest_catalog}.{dest_schema}.ref_hcpcs"
revenue_bronze_table     = f"{dest_catalog}.{dest_schema}.ref_revenue_codes"
pos_bronze_table         = f"{dest_catalog}.{dest_schema}.ref_place_of_service"

source_path = dbutils.widgets.get("reference_volume_path")
write_mode = dbutils.widgets.get("write_mode").lower().strip()
validate_counts = dbutils.widgets.get("validate_counts").lower().strip() == "false"

# Run log table
run_log_table            = f"{dest_catalog}.{dest_schema}.pipeline_run_log"

raw_file_full_path = f"{dbutils.widgets.get('reference_volume_path')}HCPC_ICD10_Place_of_Service_Diagnosis_Codes.xlsx"
raw_file_full_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Create the Run Log Table (if not exists)

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
  error_message STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Derived Variables + Run Context

# COMMAND ----------

source_table_member = (
    f"{dbutils.widgets.get('source_catalog')}."
    f"{dbutils.widgets.get('source_schema')}."
    "members"
)
source_table_membership_month = (
    f"{dbutils.widgets.get('source_catalog')}."
    f"{dbutils.widgets.get('source_schema')}."
    "membership_month"
)

source_table_claim_header = (
    f"{dbutils.widgets.get('source_catalog')}."
    f"{dbutils.widgets.get('source_schema')}."
    "claims_header"
)
source_table_claim_diagnosis = (
    f"{dbutils.widgets.get('source_catalog')}."
    f"{dbutils.widgets.get('source_schema')}."
    "claims_diagnosis"
)
source_table_claim_line = (
    f"{dbutils.widgets.get('source_catalog')}."
    f"{dbutils.widgets.get('source_schema')}."
    "claims_lines"
)
source_table_providers = (
    f"{dbutils.widgets.get('source_catalog')}."
    f"{dbutils.widgets.get('source_schema')}."
    "providers"
)

print(f"run_id: {run_id}")
print(f"source_table: {source_table_member}")
print(f"source_table: {source_table_claim_header}")
print(f"source_table: {source_table_claim_diagnosis}")
print(f"source_table: {source_table_claim_line}")
print(f"source_table: {source_table_providers}") 
print(f"source_path: {source_path}")
print(f"write_mode: {write_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Helper Functions (Write + Logging)

# COMMAND ----------

import re

def write_delta(df, target_table: str, mode: str = "append", partition_cols: list[str] | None = None) -> int:
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

def log_run(
    pipeline_name: str,
    layer: str,
    source: str,
    target: str,
    status: str,
    start_time_dt: datetime,
    rows_read: int,
    rows_written: int,
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
        error_message
    )], schema="""
        run_id string, pipeline_name string, layer string, source string, target string,
        status string, start_time timestamp, end_time timestamp, duration_seconds double,
        rows_read long, rows_written long, error_message string
    """)

    (log_df.write
          .format("delta")
          .mode("append")
          .saveAsTable(run_log_table))
    
def clean_columns(df):
    df.columns = (
        df.columns
          .str.strip()                    # remove leading/trailing spaces
          .str.lower()                    # lowercase
          .str.replace(r'[^a-z0-9]', '_', regex=True)  # replace ALL non-alphanumeric with _
          .str.replace(r'_+', '_', regex=True)         # collapse multiple underscores
          .str.strip('_')                 # remove leading/trailing underscores
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Ingest Claims (Bronze: raw + metadata only)

# COMMAND ----------

import pandas as pd

def bronze_ref_from_pandas(pdf: pd.DataFrame, target_table: str, source_name: str):
    """Convert a pandas dataframe to Spark and write to Delta with Bronze metadata."""
    df = spark.createDataFrame(pdf)
    df = (df
          .withColumn("run_id", F.lit(run_id))
          .withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_file", F.lit(source_name)))
    return write_delta(df, target_table, mode=write_mode)

def bronze_ref_from_spark(df, target_table: str, source_name: str) -> int:
    """Write a Spark dataframe to Delta with Bronze metadata."""
    df2 = (df
           .withColumn("run_id", F.lit(run_id))
           .withColumn("ingestion_timestamp", F.current_timestamp())
           .withColumn("source_file", F.lit(source_name)))
    return write_delta(df2, target_table, mode=write_mode)

# COMMAND ----------

pipeline_name = "claims_analytics_Pipeline"
layer = "bronze"

for file_name in ["members","membership_month","claims_header","claims_diagnosis","claims_lines","providers"]:
    bronze_table= f"bronze_analytics.claims_analytics.{file_name}"
    
    file_path = f"{file_name}.csv"
    file_path_source = source_path + file_path

    print(f"{bronze_table} {file_path} {file_path_source}")

    rows_read = 0
    rows_written = 0
    ref_start = datetime.now()

    try:
        
        df_src = (spark.read
            .option('header', 'true')
            .option('inferSchema', 'false')
            .csv(file_path_source))
        rows_read = df_src.count()
        rows_written = bronze_ref_from_spark(df_src, bronze_table, file_path)

        log_run(pipeline_name, layer, file_path_source, bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
        print(f"{file_name} loaded. rows_read={rows_read}, rows_written={rows_written}")

    except Exception as e:
        log_run(pipeline_name, layer, file_path_source, bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
        raise

payers = [(1,"Medicaid"), (2,"Commercial"), (3,"Medicare"), (4,"Unspecified"), (5,"Medicare Supplement")]
payer_df = pd.DataFrame(payers, columns=['payer_id','payer_name'])
rows_read = len(payer_df)
payer_start = datetime.now()
rows_written = bronze_ref_from_pandas(payer_df, f"{dest_catalog}.{dest_schema}.payers", 'Payers')
log_run(pipeline_name, layer, 'Payer Hard Coded', f"{dest_catalog}.{dest_schema}.payers", "SUCCESS", payer_start, rows_read, rows_written)
print(f"Payer loaded. rows_read={rows_read}, rows_written={rows_written}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Ingest Reference Tables (small lookups)
# MAGIC Typically for smaller fiels, Pandas is acceptable over Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 ICD-10 Diagnosis Codes

# COMMAND ----------

import os
import openpyxl
from pyspark.sql.functions import substr, col, lit, when, length, concat, trim

rows_read = 0
rows_written = 0
ref_start = datetime.now()

try:
    
    df_icd10_file = spark.createDataFrame(pd.read_excel(raw_file_full_path, sheet_name="ref_icd10_diagnosis").astype(str)) # Diagnosis Codes 
    rows_read = df_icd10_file.count()

    # Keep only necessary column(s) + metadata
    df_icd10_out = df_icd10_file.select(F.col('DIAGNOSIS_CODE').alias('diagnosis_code'),"short_description","long_description")
    rows_written = bronze_ref_from_spark(df_icd10_out, icd10_bronze_table, raw_file_full_path)

    log_run(pipeline_name, layer, raw_file_full_path, icd10_bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
    print(f"ICD10 loaded. rows_read={rows_read}, rows_written={rows_written}")

except Exception as e:
    log_run(pipeline_name, layer, raw_file_full_path, icd10_bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
    raise
df_icd10_file

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 HCPCS / Procedure Codes

# COMMAND ----------

rows_read = 0
rows_written = 0
ref_start = datetime.now()

try:

    pdf_hcpcs = pd.read_excel(raw_file_full_path,sheet_name="ref_hcpcs").astype(str) # Procedure Codes
    rows_read = len(pdf_hcpcs)
    
    pdf_hcpcs = clean_columns(pdf_hcpcs)

    rows_written = bronze_ref_from_pandas(pdf_hcpcs, hcpcs_bronze_table, raw_file_full_path)

    log_run(pipeline_name, layer, raw_file_full_path, hcpcs_bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
    print(f"HCPCS loaded. rows_read={rows_read}, rows_written={rows_written}")

except Exception as e:
    log_run(pipeline_name, layer, raw_file_full_path, hcpcs_bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
    raise
# pdf_hcpcs

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Revenue Codes

# COMMAND ----------

rows_read = 0
rows_written = 0
ref_start = datetime.now()

try:    
    # pdf_rev = pd.read_excel(
    #     pd.read_excel(raw_file_full_path, sheet_name="ref_revenue_codes"),
    #     names=["Revenue_CODE", "DESCRIPTION"]
    # ).astype(str)
    
    pdf_rev = pd.read_excel(raw_file_full_path, sheet_name="ref_revenue_codes", names=["Revenue_CODE", "DESCRIPTION"]).astype(str) # Revenue Codes
    rows_read = len(pdf_rev)

    pdf_rev = clean_columns(pdf_rev)

    rows_written = bronze_ref_from_pandas(pdf_rev, revenue_bronze_table, raw_file_full_path)

    log_run(pipeline_name, layer, raw_file_full_path, revenue_bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
    print(f"Revenue codes loaded. rows_read={rows_read}, rows_written={rows_written}")

except Exception as e:
    log_run(pipeline_name, layer, raw_file_full_path, revenue_bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
    raise
# pdf_rev

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Place of Service (CSV)

# COMMAND ----------

rows_read = None
rows_written = None

ref_start = datetime.now()

try:
    raw_pos_file_path = dbutils.widgets.get("reference_volume_path")

    df_pos_file   = pd.read_excel(raw_file_full_path, sheet_name='Place of Services').astype(str)
    
    # Rename a column in a pandas DataFrame
    df_pos_file = df_pos_file.rename(columns={'Place of Service Code(s)': 'Place_of_Service_Code',
                                    "Place of Service Name": 'Place_of_Service_Name', 
                                    "Place of Service Description": 'Place_of_Service_Description'}
                                    )
    
    df_pos = spark.createDataFrame(df_pos_file)
    rows_read = df_pos.count()

    df_pos_bronze = (df_pos
                     .withColumn("run_id", F.lit(run_id))
                     .withColumn("ingestion_timestamp", F.current_timestamp())
                     .withColumn("source_file", F.lit(f"{raw_pos_file_path}Place of Services.csv")))

    rows_written = write_delta(df_pos_bronze, pos_bronze_table, mode=write_mode)

    log_run(pipeline_name, layer, raw_pos_file_path, pos_bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
    print(f"Place of service loaded. rows_read={rows_read}, rows_written={rows_written}")
    print (rows_read)

except Exception as e:
    log_run(pipeline_name, layer, raw_pos_file_path, pos_bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Basic Validation (No EDA)
# MAGIC Only confirm that tables exist and contain records. Detailed profiling belongs in Silver/Gold or a separate profiling notebook.

# COMMAND ----------

tables = [
    source_table_member,
    source_table_membership_month,
    source_table_claim_header,
    source_table_claim_diagnosis,
    source_table_claim_line,
    source_table_providers,
    icd10_bronze_table,
    hcpcs_bronze_table,
    revenue_bronze_table,
    pos_bronze_table,
    f"{dest_catalog}.{dest_schema}.payers",
    run_log_table
]

for t in tables:
    exists = spark.catalog.tableExists(t)
    cnt = spark.table(t).count() if (exists and validate_counts) else (None if exists else 0)
    print(f"{t}: exists={exists}, rows={cnt if validate_counts else 'skipped'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9) Review Latest Run Log

# COMMAND ----------

(spark.table(run_log_table)
      .orderBy(F.col("end_time").desc())
      .limit(50)
      .display()
)