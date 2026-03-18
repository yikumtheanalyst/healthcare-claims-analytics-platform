# Databricks notebook source
# MAGIC %md
# MAGIC # Project Type: Enterprise-Style Synthetic Healthcare Claims Data Platform

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 — Gold Layer: healthcare Claims Star Schema + Power BI Marts (Clean Template)
# MAGIC
# MAGIC **Purpose:** Build **business-ready** Gold tables from Silver entities (Facts, Dimensions, and BI marts/views).  
# MAGIC **Architecture:** Medallion (Bronze → Silver → Gold)
# MAGIC
# MAGIC ## Gold Principles Applied
# MAGIC - Read from Silver clean entity tables (trusted data)
# MAGIC - Apply **business rules** and modeling decisions (grain, surrogate keys, conformed dimensions)
# MAGIC - Create **Facts + Dimensions** optimized for BI (Power BI)
# MAGIC - Create optional marts/views for performance and ease of use
# MAGIC - Structured run logging with **run-level** metrics
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Imports

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Parameters (Widgets)

# COMMAND ----------

# Clear widgets (safe for interactive use; harmless in jobs)
dbutils.widgets.removeAll()

# COMMAND ----------

# Silver source
dbutils.widgets.text("source_catalog", "silver_analytics")          # catalog
dbutils.widgets.text("source_schema", "claims_analytics")       # schema

# Gold destination
dbutils.widgets.text("destination_catalog", "gold_analytics")       # catalog
dbutils.widgets.text("destination_schema", "claims_analytics")  # schema

# Which Silver batch to process
# - "latest" = process most recent silver_run_id from claims_clean
# - or provide an explicit silver_run_id UUID
dbutils.widgets.text("process_silver_run_id", "latest")

# Gold write mode: typically overwrite (rebuild marts/dims/facts each run)
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Derived Variables + Run Context

# COMMAND ----------

gold_run_id = str(uuid.uuid4())
start_time = datetime.now()

src_catalog = dbutils.widgets.get("source_catalog")
src_schema  = dbutils.widgets.get("source_schema")

dst_catalog = dbutils.widgets.get("destination_catalog")
dst_schema  = dbutils.widgets.get("destination_schema")

process_silver_run_id = dbutils.widgets.get("process_silver_run_id").strip().lower()
write_mode = dbutils.widgets.get("write_mode").strip().lower()

# Silver source tables 
silver_claims_header_table    = f"{src_catalog}.{src_schema}.claims_header"
silver_claims_line_table      = f"{src_catalog}.{src_schema}.claims_clean"
silver_bridge_claim_diagnosis = f"{src_catalog}.{src_schema}.claims_diagnosis"
silver_claims_rejects         = f"{src_catalog}.{src_schema}.claims_rejects"
silver_diagnosis_table        = f"{src_catalog}.{src_schema}.ref_icd10_diagnosis"
silver_hcpcs_table          = f"{src_catalog}.{src_schema}.ref_hcpcs"
silver_rev_table            = f"{src_catalog}.{src_schema}.ref_revenue_codes"
silver_pos_table            = f"{src_catalog}.{src_schema}.ref_place_of_service"
silver_members_table        = f"{src_catalog}.{src_schema}.members"
silver_membership_month_table = f"{src_catalog}.{src_schema}.membership_month"
silver_providers_table      = f"{src_catalog}.{src_schema}.providers"
silver_payers_table         = f"{src_catalog}.{src_schema}.payers"


# Gold star schema tables
gold_fact_claim_header      = f"{dst_catalog}.{dst_schema}.fact_claim_header"
gold_fact_claim_line        = f"{dst_catalog}.{dst_schema}.fact_claim_line"
gold_bridge_claim_diagnosis = f"{dst_catalog}.{dst_schema}.bridge_claim_diagnosis"
gold_dim_members            = f"{dst_catalog}.{dst_schema}.dim_members"
gold_fact_membership        = f"{dst_catalog}.{dst_schema}.fact_membership"
gold_dim_providers          = f"{dst_catalog}.{dst_schema}.dim_providers"
gold_dim_payers             = f"{dst_catalog}.{dst_schema}.dim_payers"

gold_dim_diagnosis_codes    = f"{dst_catalog}.{dst_schema}.dim_diagnosis_codes"
gold_dim_procedures_codes   = f"{dst_catalog}.{dst_schema}.dim_procedures_codes"
gold_dim_revenue_codes      = f"{dst_catalog}.{dst_schema}.dim_revenue_codes"
gold_dim_place_of_service   = f"{dst_catalog}.{dst_schema}.dim_place_of_service"

gold_dim_date               = f"{dst_catalog}.{dst_schema}.dim_date"

# Gold marts (optional)
gold_mart_payer_month    = f"{dst_catalog}.{dst_schema}.mart_plan_month"

# Gold run log + DQ issues table
run_log_table           = f"{dst_catalog}.{dst_schema}.pipeline_run_log"
dq_issues_table         = f"{dst_catalog}.{dst_schema}.dq_issues"

print(f"gold_run_id: {gold_run_id}")
print(f"process_silver_run_id: {process_silver_run_id}")
print(f"write_mode: {write_mode}")
print(f"silver_claims_header_table: {silver_claims_header_table}")
print(f"silver_claims_line_table: {silver_claims_line_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Create Gold Run Log + DQ Tables (if not exists)

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

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {dq_issues_table} (
  gold_run_id STRING,
  issue_timestamp TIMESTAMP,
  issue_type STRING,
  table_name STRING,
  record_count LONG,
  details STRING
)
USING DELTA
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Helper Functions (Logging, Writing, Keys, DQ)

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
    rows_rejected: int = 0,
    error_message: str = None
):
    end_time_dt = datetime.now()
    duration_seconds = (end_time_dt - start_time_dt).total_seconds()

    log_df = spark.createDataFrame([(
        gold_run_id,
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
        int(rows_rejected) if rows_rejected is not None else 0,
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


def write_delta(df, target_table: str, mode: str = "overwrite") -> int:
    """Write a dataframe to Delta and return rows written for THIS write."""
    rows_written = df.count()
    (df.write
       .format("delta")
       .mode(mode)
       .option("overwriteSchema", "true" if mode == "overwrite" else "false")
       .option("mergeSchema", "true")
       .saveAsTable(target_table))
    return rows_written


def stable_key(*cols) -> F.Column:
    """Create a deterministic surrogate key from one or more business key columns."""
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return F.sha2(F.concat_ws("||", *exprs), 256)


def record_dq(issue_type: str, table_name: str, record_count: int, details: str):
    df_issue = spark.createDataFrame([(
        gold_run_id,
        datetime.now(),
        issue_type,
        table_name,
        int(record_count),
        details
    )], schema="""
        gold_run_id string, issue_timestamp timestamp, issue_type string,
        table_name string, record_count long, details string
    """)
    (
    df_issue.write
        .format("delta")
        .mode("append")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable(dq_issues_table)
)


def get_latest_silver_run_id(df_silver):
    if "silver_run_id" not in df_silver.columns or "silver_processed_timestamp" not in df_silver.columns:
        raise ValueError("Silver table must contain silver_run_id and silver_processed_timestamp columns.")
    r = (df_silver
         .orderBy(F.col("silver_processed_timestamp").desc())
         .select("silver_run_id")
         .limit(1)
         .collect())
    if not r:
        raise ValueError("Silver claims table is empty.")
    return r[0]["silver_run_id"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Read Silver Claims and Select Batch to Process

# COMMAND ----------

# Claim Header

if not spark.catalog.tableExists(silver_claims_header_table):
    raise ValueError(f"Silver claims table not found: {silver_claims_header_table}")

df_claims_silver_header = spark.table(silver_claims_header_table)
if process_silver_run_id == "latest":
    silver_run_id_to_process = get_latest_silver_run_id(df_claims_silver_header)
else:
    silver_run_id_to_process = dbutils.widgets.get("process_silver_run_id").strip()

df_claims_silver = df_claims_silver_header.filter(F.col("silver_run_id") == silver_run_id_to_process)

rows_claims_read = df_claims_silver_header.count()
if rows_claims_read == 0:
    raise ValueError(f"No rows found for silver_run_id={silver_run_id_to_process} in {silver_claims_header_table}")

print(f"Processing silver_run_id: {silver_run_id_to_process}")
print(f"claims rows_read: {rows_claims_read}")

# Claim Line
if not spark.catalog.tableExists(silver_claims_line_table):
    raise ValueError(f"Silver claims table not found: {silver_claims_line_table}")

df_claims_line_silver = spark.table(silver_claims_line_table)
df_claims_line_silver = df_claims_line_silver.withColumnRenamed("place_of_service_std_id", "place_of_service_code")

if process_silver_run_id == "latest":
    silver_run_id_to_process = get_latest_silver_run_id(df_claims_line_silver)
else:
    silver_run_id_to_process = dbutils.widgets.get("process_silver_run_id").strip()

df_claims_silver = df_claims_line_silver.filter(F.col("silver_run_id") == silver_run_id_to_process)

rows_claims_read = df_claims_silver.count()
if rows_claims_read == 0:
    raise ValueError(f"No rows found for silver_run_id={silver_run_id_to_process} in {silver_claims_line_table}")

print(f"Processing silver_run_id: {silver_run_id_to_process}")
print(f"claims rows_read: {rows_claims_read}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Apply Gold Business Rules (Derived Measures + Flags)
# MAGIC Gold is where domain rules belong. Keep original Silver columns and add derived columns/flags.

# COMMAND ----------

pipeline_name = "claims_analytics_Pipeline"
layer = "gold"

# Fact Claim Lines
claims_header_df = df_claims_silver_header

# Example business rule: impute null monetary amounts to 0 (transparent flags)
money_cols = ["paid_amount", "allowed_amount", "billed_amount", "line_paid", "line_allowed", "line_charge", "member_cost_share"]
for c in money_cols:
    if c in claims_header_df.columns:
        claims_header_df = claims_header_df.withColumn(f"{c}_imputed_flag", F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0)))
        claims_header_df = claims_header_df.withColumn(c, F.coalesce(F.col(c), F.lit(0.00).cast("decimal(18,2)")))

# Null units/quantity => 0 (transparent flags)
for c in ["units", "quantity", "days_supply"]:
    if c in claims_header_df.columns:
        claims_header_df = claims_header_df.withColumn(f"{c}_imputed_flag", F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0)))
        claims_header_df = claims_header_df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

# Claim type flag
if "claim_type" in claims_header_df.columns:
    claims_header_df = claims_header_df.withColumn("is_rx_claim", F.when(F.upper(F.col("claim_type")).isin("RX","PHARMACY"), F.lit(1)).otherwise(F.lit(0)))
else:
    claims_header_df = claims_header_df.withColumn("is_rx_claim", F.lit(None).cast("int"))

# Gold metadata
claims_header_df = (claims_header_df
      .withColumn("gold_run_id", F.lit(gold_run_id))
      .withColumn("gold_processed_timestamp", F.current_timestamp())
     )

# Fact Claim Lines
# claims_lines_df = df_claims_line_silver

# Example business rule: impute null monetary amounts to 0 (transparent flags)
money_cols = ["paid_amount", "allowed_amount", "billed_amount", "line_paid", "line_allowed", "line_charge", "member_cost_share"]
for c in money_cols:
    if c in df_claims_line_silver.columns:
        df_claims_line_silver = df_claims_line_silver.withColumn(f"{c}_imputed_flag", F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0)))
        df_claims_line_silver = df_claims_line_silver.withColumn(c, F.coalesce(F.col(c), F.lit(0.00).cast("decimal(18,2)")))

# Null units/quantity => 0 (transparent flags)
for c in ["units", "quantity", "days_supply"]:
    if c in df_claims_line_silver.columns:
        df_claims_line_silver = df_claims_line_silver.withColumn(f"{c}_imputed_flag", F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0)))
        df_claims_line_silver = df_claims_line_silver.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

# Claim type flag
if "claim_type" in df_claims_line_silver.columns:
    df_claims_line_silver = df_claims_line_silver.withColumn("is_rx_claim", F.when(F.upper(F.col("claim_type")).isin("RX","PHARMACY"), F.lit(1)).otherwise(F.lit(0)))
else:
    df_claims_line_silver = df_claims_line_silver.withColumn("is_rx_claim", F.lit(None).cast("int"))

# Gold metadata
df_claims_line_silver = (df_claims_line_silver
      .withColumn("gold_run_id", F.lit(gold_run_id))
      .withColumn("gold_processed_timestamp", F.current_timestamp())
     )

print (claims_header_df.count() , df_claims_line_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Build Dimensions (Conformed)

# COMMAND ----------

# Determine candidate columns based on available schema

provider_cols       = [c for c in ['rendering_npi', 'billing_npi', 'provider_name','taxonomies', 'provider_specialty'] if c in spark.table(silver_providers_table).columns]
diagnosis_cols      = [c for c in ['diagnosis_code','short_description','long_description'] if c in spark.table(silver_diagnosis_table).columns]
hcps_df = spark.table(silver_hcpcs_table)
hcps_df = hcps_df.withColumnRenamed("hcpc", "procedure_code")

placeofservice_cols = [c for c in ['Place_of_Service_Code', 'Place_of_Service_Name', 'Place_of_Service_Description'] if c in spark.table(silver_pos_table).columns]

def build_dim(source_df, id_col: str, cols: list, key_col: str, key_from: list, target_table: str):
    if id_col not in source_df.columns:
        print(f"{id_col} not found; skipping {target_table}")
        return 0
    dim_df = (source_df.select(*cols)
              .dropDuplicates([id_col])
              .withColumn(key_col, stable_key(*key_from))
              .withColumn("gold_run_id", F.lit(gold_run_id))
              .withColumn("gold_processed_timestamp", F.current_timestamp()))
    return write_delta(dim_df, target_table, mode=write_mode)


# Dim Payers
t0 = datetime.now()
try:
    payers_df = spark.table(silver_payers_table)
    # Rename payer_name as payer_type
    payers_df = payers_df.withColumnRenamed("payer_name", "payer_type")
    payers_cols = [c for c in ['payer_id','payer_type'] if c in payers_df.columns]

    rw = build_dim(payers_df,"payer_type", payers_cols, "payer_key", ["payer_type"],  gold_dim_payers)
    log_run(pipeline_name, layer, f"{silver_claims_header_table} (silver_run_id={silver_run_id_to_process})", gold_dim_payers, "SUCCESS", t0, rows_claims_read, rw, 0)
    
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_header_table, gold_dim_payers, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# members
t0 = datetime.now()
try:
    member_df = spark.table(silver_members_table).select('member_id', 'gender', 'birth_date', 'age','state', 'zip3', 'payer_type').distinct()
    members_cols       = [c for c in ['member_id', 'gender', 'birth_date','age', 'state', 'zip3', 'payer_type'] if c in member_df.columns]
    rw = build_dim(member_df,"member_id", members_cols, "member_key", ["member_id"], gold_dim_members)
    log_run(pipeline_name, layer, f"{silver_claims_header_table} (silver_run_id={silver_run_id_to_process})", gold_dim_members, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_header_table, gold_dim_members, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim provider
t0 = datetime.now()
try:
    prov_df = spark.table(silver_providers_table)
    rw = build_dim(prov_df,"rendering_npi", provider_cols, "provider_key", ["rendering_npi"], gold_dim_providers)
    log_run(pipeline_name, layer, f"{silver_claims_header_table} (silver_run_id={silver_run_id_to_process})", gold_dim_providers, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_header_table, gold_dim_providers, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Diagnosis 
t0 = datetime.now()
try:
    diag_df = spark.table(silver_diagnosis_table)
    rw = build_dim(diag_df,"diagnosis_code", diagnosis_cols, "diagnosis_code_key", ["diagnosis_code"], gold_dim_diagnosis_codes)
    log_run(pipeline_name, layer, f"{silver_bridge_claim_diagnosis} (silver_run_id={silver_run_id_to_process})", gold_dim_diagnosis_codes, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_bridge_claim_diagnosis, gold_dim_diagnosis_codes, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Place of Service
t0 = datetime.now()
try:
    pos_df = spark.table(silver_pos_table)
    rw = build_dim(pos_df,"Place_of_Service_Code", placeofservice_cols, "place_of_service_key", ["Place_of_Service_Code"],  gold_dim_place_of_service)
    log_run(pipeline_name, layer, f"{silver_claims_header_table} (silver_run_id={silver_run_id_to_process})", gold_dim_place_of_service, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_header_table, gold_dim_place_of_service, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10) Build dim_date

# COMMAND ----------

t0 = datetime.now()
try:
    if "service_from_date" not in spark.table(silver_claims_header_table).columns:
        raise ValueError("service_from_date missing; cannot build dim_date.")

    mm = spark.table(silver_claims_header_table).select(F.min("service_from_date").cast("date").alias("min_d"), F.max("service_from_date").cast("date").alias("max_d")).collect()[0]
    min_d, max_d = mm["min_d"], mm["max_d"]
    if min_d is None or max_d is None:
        raise ValueError("No non-null service_from_date values found.")

    df_dates = spark.sql(f"SELECT explode(sequence(to_date('{min_d}'), to_date('{max_d}'), interval 1 day)) AS date_day")

    df_dim_date = (df_dates
    .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
    .withColumn("actual_date", F.date_format("date_day", "yyyy-MM-dd").cast("date"))
    .withColumn("year", F.year("date_day"))
    .withColumn("quarter", F.quarter("date_day"))
    .withColumn("month", F.month("date_day"))
    .withColumn("month_name", F.date_format("date_day", "MMMM"))
    .withColumn("day", F.dayofmonth("date_day"))
    .withColumn("day_of_week_num", F.dayofweek("date_day"))
    .withColumn("day_of_week_name", F.date_format("date_day", "E"))
    .withColumn("week_of_year", F.weekofyear("date_day"))
    .withColumn("is_weekend", F.when(F.col("day_of_week_num").isin([1,7]), 1).otherwise(0)  )
    .withColumn("gold_run_id", F.lit(gold_run_id))
    .withColumn("gold_processed_timestamp", F.current_timestamp())
)    

    rw = write_delta(df_dim_date, gold_dim_date, mode=write_mode)
    log_run(pipeline_name, layer, f"{silver_claims_header_table} (silver_run_id={silver_run_id_to_process})", gold_dim_date, "SUCCESS", t0, rows_claims_read, rw, 0)

except Exception as e:
    log_run(pipeline_name, layer, silver_claims_header_table, gold_dim_date, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11) Build fact_claim_header (Grain + Measures + Keys)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 11.5) Setup the Surrogate keys for dimmensions

# COMMAND ----------

t0 = datetime.now()
try:
    # Determine grain key
    if "claim_id" in claims_header_df.columns:
        df_fact_claim_header = claims_header_df.withColumn("claim_fact_header_key", stable_key("claim_id"))
        grain_cols = ["claim_id"]
    else:
        # Fallback grain
        required = [c for c in ["claim_id",'service_from_date'] if c in claims_header_df.columns]
        if len(required) < 2:
            raise ValueError("Cannot determine fact grain. Need claim_id/member_id (and ideally service_from_date).")
        df_fact_claim_header = claims_header_df.withColumn("claim_fact_header_key", stable_key(*required))
        grain_cols = required

    # Surrogate keys for dimensions    
    dt = spark.table(gold_dim_date).select("date_key", F.col("actual_date").alias("service_from_date"))
    df_fact_claim_header = df_fact_claim_header.alias("ch").join(spark.table(gold_dim_payers).alias("py"), on =["payer_type"], how="left").select("ch.*","py.payer_key")
    df_fact_claim_header = df_fact_claim_header.alias("ch").join(spark.table(gold_dim_members).alias("m"), on =["member_id"], how="left").select("ch.*","m.member_key")
    df_fact_claim_header = df_fact_claim_header.alias("ch").join(spark.table(gold_dim_providers).alias("p").select("rendering_npi","provider_key"), on =["rendering_npi"], how="left").select("ch.*","p.provider_key")
    df_fact_claim_header = df_fact_claim_header.alias("ch").join(spark.table(gold_dim_place_of_service).alias("sp"), on =["place_of_service_code"], how="left").select("ch.*","sp.place_of_service_key")
    df_fact_claim_header = df_fact_claim_header.alias("ch").join(dt, on =["service_from_date"], how="left").select("ch.*","date_key") 
    
    # Date key
    df_fact_claim_header = df_fact_claim_header.withColumn("date_key", F.date_format(F.col("service_from_date"), "yyyyMMdd").cast("int"))

    # DQ: duplicate grain count
    dup_cnt = (df_fact_claim_header.groupBy(*grain_cols).count().filter(F.col("count") > 1).count())
    if dup_cnt > 0:
        record_dq("DUPLICATE_GRAIN", gold_fact_claim_header, dup_cnt, f"Duplicate grain found on {grain_cols}")
        print(f"WARNING: duplicate grain rows found: {dup_cnt}")

    # Select columns for the fact table
    keep = ["claim_id","claim_fact_header_key","payer_key", "member_key", "provider_key","place_of_service_key", "date_key","service_from_date","service_to_date","is_rx_claim","claim_type" ]

    df_fact_claim_header_out = (df_fact_claim_header.select(*keep)
                   .withColumn("gold_run_id", F.lit(gold_run_id))
                   .withColumn("gold_processed_timestamp", F.current_timestamp()))

    rw = write_delta(df_fact_claim_header_out, gold_fact_claim_header, mode=write_mode)
    log_run(pipeline_name, layer, f"{silver_claims_header_table} (silver_run_id={silver_run_id_to_process})", gold_fact_claim_header, "SUCCESS", t0, rows_claims_read, rw, 0)

except Exception as e:
    log_run(pipeline_name, layer, silver_claims_header_table, gold_fact_claim_header, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.5) Build fact_claim_line (Grain + Measures + Keys)

# COMMAND ----------

spark.table(silver_rev_table).columns

# COMMAND ----------

t0 = datetime.now()
try:
    # Determine grain key
    df_claims_line_silver = spark.table(silver_claims_line_table)
    if "claim_id" in df_claims_line_silver.columns:
        df_claims_line_silver = df_claims_line_silver.withColumn("claim_fact_line_key", stable_key("claim_id"))
        grain_cols = ["claim_id","service_line_number"]
        
    else:
        # Fallback grain
        required = [c for c in ["claim_id", "service_line_number"] if c in df_claims_line_silver.columns]
        if len(required) < 2:
            raise ValueError("Cannot determine fact grain. Need claim_id/member_id (and ideally service_from_date).")
        df_claims_line_silver = df_claims_line_silver.withColumn("claim_fact_line_key", stable_key(*required))
        grain_cols = required

    # DQ: duplicate grain count
    dup_cnt = (df_claims_line_silver.groupBy(*grain_cols).count().filter(F.col("count") > 1).count())
    if dup_cnt > 0:
        record_dq("DUPLICATE_GRAIN", gold_fact_claim_line, dup_cnt, f"Duplicate grain found on {grain_cols}")
        print(f"WARNING: duplicate grain rows found: {dup_cnt}")

    # Dim Procedure Codes
    hcpcs_icd10_cols      = [c for c in ['procedure_code', 'long_description', 'short_description',] if c in hcps_df.columns]
    t0 = datetime.now()
    try:    
        rw = build_dim(hcps_df,"procedure_code", hcpcs_icd10_cols, "procedure_code_key", ["procedure_code"], gold_dim_procedures_codes)
        log_run(pipeline_name, layer, f"{silver_claims_line_table} (silver_run_id={silver_run_id_to_process})", gold_dim_procedures_codes, "SUCCESS", t0, rows_claims_read, rw, 0)
    except Exception as e:
        log_run(pipeline_name, layer, silver_claims_line_table, gold_dim_procedures_codes, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

    # Dim Revenue Codes
    rev_df = spark.table(silver_rev_table)
    revenue_codes_cols  = [c for c in ['revenue_code','description'] if c in spark.table(silver_rev_table).columns]

    t0 = datetime.now()
    try: 
        rw = build_dim(rev_df,"revenue_code", revenue_codes_cols, "revenue_code_key", ["revenue_code"],  gold_dim_revenue_codes)
        log_run(pipeline_name, layer, f"{silver_claims_line_table} (silver_run_id={silver_run_id_to_process})", gold_dim_revenue_codes, "SUCCESS", t0, rows_claims_read, rw, 0)
    except Exception as e:
        log_run(pipeline_name, layer, silver_claims_line_table, gold_dim_revenue_codes, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

    
    # Surrogate keys for dimensions
    df_claims_line_silver = df_claims_line_silver.alias("cl").join(spark.table(gold_fact_claim_header), on ='claim_id', how = "left").select("cl.*","claim_fact_header_key","date_key")
    df_claims_line_silver = df_claims_line_silver.alias("cl").join(spark.table(gold_dim_procedures_codes), on ='procedure_code', how = "left").select("cl.*","procedure_code_key")
    df_claims_line_silver = df_claims_line_silver.alias("cl").join(spark.table(gold_dim_revenue_codes), on ='revenue_code', how = "left").select("cl.*","revenue_code_key")


    # Select columns for the fact table
    keep = ["claim_fact_line_key", "claim_fact_header_key","date_key","claim_id", "service_line_number", "procedure_code_key","revenue_code_key"]

    measures = ["paid_amount","allowed_amount","billed_amount","line_paid","line_allowed","line_charge","member_cost_share",
                "units","quantity","days_supply"]
    for c in measures:
        if c in df_claims_line_silver.columns:
            keep.append(c)

    keep = [c for c in keep if c in df_claims_line_silver.columns]

    df_fact_line_out = (df_claims_line_silver.select(*keep)
                   .withColumn("gold_run_id", F.lit(gold_run_id))
                   .withColumn("gold_processed_timestamp", F.current_timestamp()))

    rw = write_delta(df_fact_line_out, gold_fact_claim_line, mode=write_mode)
    log_run(pipeline_name, layer, f"{df_claims_line_silver} (silver_run_id={silver_run_id_to_process})", gold_fact_claim_line, "SUCCESS", t0, rows_claims_read, rw, 0)

except Exception as e:
    log_run(pipeline_name, layer, df_claims_line_silver, gold_fact_claim_line, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.5) Build bridge claim diagnosis

# COMMAND ----------

spark.table(silver_bridge_claim_diagnosis).columns

# COMMAND ----------

try:    
    df_bridge_claim_diag = spark.table(silver_bridge_claim_diagnosis)

    df_fact_header = spark.table(gold_fact_claim_header).select(
        'claim_id',
        'claim_fact_header_key',
        "date_key"
    )

    df_dim_diag = spark.table(gold_dim_diagnosis_codes).select(
        'diagnosis_code',
        'diagnosis_code_key'
    )

    df_bridge = (
        df_bridge_claim_diag
        .join(df_fact_header, 'claim_id', 'inner')
        .join(df_dim_diag, 'diagnosis_code', 'inner')
        .select(
            'claim_fact_header_key',
            "date_key",
            'diagnosis_code_key',
            'diagnosis_seq'
        )
    )

    rw = write_delta(df_bridge, gold_bridge_claim_diagnosis, mode=write_mode)
    log_run(pipeline_name, layer, f"{gold_fact_claim_header} (silver_run_id={silver_run_id_to_process})", gold_bridge_claim_diagnosis, "SUCCESS", t0, rows_claims_read, rw, 0)

except Exception as e:
    log_run(pipeline_name, layer, gold_fact_claim_header, gold_bridge_claim_diagnosis, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 12) Build Membership Fact Table
# MAGIC #### Enrollment is a time-dependent, not a static demographic attribue.  
# MAGIC #### Dim_members table holds attribues that do not change for a member often, such as name, gender, dob, etc.

# COMMAND ----------

# DBTITLE 1,Build Membership Fact Table
import random
import pandas as pd

try:
  enrollment_df = spark.table(silver_membership_month_table).join(spark.table(gold_dim_members).select('member_id','member_key'),on='member_id', how='inner')
  payers_df = spark.table(gold_dim_payers) 
  membership_payers_df = (
    enrollment_df.alias("e").join(payers_df.alias("p"), 'payer_type', 'left')
      .select(
          'member_key',
          'payer_key',
          'Date_ID',
          F.lit(1).alias('member_count'))
      )

  membership_payers_df = membership_payers_df.withColumnRenamed("Date_ID", "year_month_date")
  rw = write_delta(membership_payers_df, gold_fact_membership, mode=write_mode)
  log_run(pipeline_name, layer, f"{gold_fact_claim_header} (silver_run_id={silver_run_id_to_process})", gold_fact_membership, "SUCCESS", t0, rows_claims_read, rw, 0)

except Exception as e:
    log_run(pipeline_name, layer, gold_fact_claim_header, gold_fact_membership, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12.5) Build Gold Marts (Aggregations for BI Performance)

# COMMAND ----------

# Mart: Plan-Month summary
t0 = datetime.now()
try:
    if not spark.catalog.tableExists(gold_fact_claim_line):
        raise ValueError(f"Gold fact claim line not found: {gold_fact_claim_line}")

    df_fact = spark.table(gold_fact_claim_line).alias("cl").join(spark.table(gold_fact_claim_header).alias("ch"), on="claim_id",how="inner") \
            .join(spark.table(gold_dim_payers).alias("p"), on="payer_key", how="left")
    rr = df_fact.count()

    if "payer_key" in df_fact.columns and "service_from_date" in df_fact.columns:
        df_mart = (df_fact
                   .withColumn("year_month", F.date_format(F.col("service_from_date"), "yyyy-MM"))
                   .groupBy("payer_key", "year_month")
                   .agg(
                       F.countDistinct("claim_id").alias("claims_cnt"),
                       F.sum(F.coalesce(F.col("line_charge"), F.lit(0))).alias("billed_amount"),
                       F.sum(F.coalesce(F.col("line_allowed"), F.lit(0))).alias("line_allowed"),                       
                       F.sum(F.coalesce(F.col("line_paid"), F.lit(0))).alias("paid_amount"),
                       F.sum(F.coalesce(F.col("line_charge"), F.lit(0)) - F.coalesce(F.col("line_paid"), F.lit(0))).alias("unpaid_amount"),
                       F.sum(F.coalesce(F.col("units"), F.lit(0))).alias("units")
                       )
                   .withColumn("gold_run_id", F.lit(gold_run_id))
                   .withColumn("gold_processed_timestamp", F.current_timestamp())
                   )

        rw = write_delta(df_mart, gold_mart_payer_month, mode=write_mode)
        log_run(pipeline_name, layer, gold_fact_claim_line, gold_mart_payer_month, "SUCCESS", t0, rr, rw, 0)
    else:
        print("payer_key/service_from_date missing; skipping mart_plan_month")

except Exception as e:
    log_run(pipeline_name, layer, gold_fact_claim_line, gold_mart_payer_month, "FAILED", t0, 0, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13) Create Business Intelligence (Power BI, Tableau, ...) Friendly Views

# COMMAND ----------

# DBTITLE 1,Claim Header Business Intelligence View
# Create/replace an enriched view for convenience (optional)
fact_claim_header_view =spark.sql(f"""
SELECT
    -- Claims
    f.claim_id 
    ,f.date_key
    ,to_date(concat(substring(f.date_key,1,4), "-", substring(f.date_key,5,2),"-",substring(f.date_key,7,2))) as claim_date
    ,f.claim_type
        
    -- Member Demographic  
    ,m.member_id
    ,m.gender as member_gender
    ,cast(m.age as int) as member_age
    ,py.payer_type
    -- Provider Demographic
    ,pr.Rendering_NPI
    ,pr.Billing_NPI
    ,pr.Taxonomies
    ,pr.Provider_Specialty
    
    -- Placce of Service
    ,pos.Place_of_Service_Code
    ,pos.Place_of_Service_Name
    ,pos.Place_of_Service_Description

    -- Financials    
    ,sum(fl.line_allowed) as line_allowed
    ,sum(fl.line_charge) as line_charge
    ,sum(fl.units) as units
    ,sum(fl.line_paid) as line_paid

FROM        {gold_fact_claim_header}  f
inner join  {gold_fact_claim_line} fl on f.claim_id = fl.claim_id
LEFT JOIN   {gold_bridge_claim_diagnosis} bcd on f.claim_fact_header_key = bcd.claim_fact_header_key
LEFT JOIN   {gold_dim_members} m ON f.member_key = m.member_key
LEFT JOIN   {gold_dim_providers} pr ON f.provider_key = pr.provider_key 
left join   {gold_dim_place_of_service} pos on f.place_of_service_key = pos.place_of_service_key
left join   {gold_dim_payers} py on f.payer_key = py.payer_key

group BY
    f.claim_id 
    ,f.date_key
    ,f.claim_type 
    ,m.member_id
    ,m.gender
    ,m.age
    ,pr.Rendering_NPI
    ,pr.Billing_NPI
    ,pr.Taxonomies
    ,pr.Provider_Specialty 
    ,pos.Place_of_Service_Code
    ,pos.Place_of_Service_Name
    ,pos.Place_of_Service_Description
     ,py.payer_type
""")

# fact_claim_header_view.display()

# Make all columns as proper case
fact_claim_header_view = fact_claim_header_view.select([F.col(x).alias(x.upper()) for x in fact_claim_header_view.columns])

# fact_claim_header_view.limit(10).display()

# fact_claim_header_view.createOrReplaceGlobalTempView(f"{dst_catalog}.{dst_schema}.vw_fact_claim_line_enriched") 
#[NOT_SUPPORTED_WITH_SERVERLESS] GLOBAL TEMPORARY VIEW is not supported on serverless compute. SQLSTATE: 0A000

# If you want to persist the view as a table, uncomment the following line:
fact_claim_header_view.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{dst_catalog}.{dst_schema}.fact_claim_header_view")

# COMMAND ----------

# DBTITLE 1,Claim Line - Procedure - Diagnosis Business Intelligence View
# Create/replace an enriched view for convenience (optional)
fact_claim_line_view =spark.sql(f"""
SELECT
    -- Claims
    f.claim_id 
    ,f.is_rx_claim
    ,f.date_key as claim_date
    ,f.claim_type
    ,fl.service_line_number
    
    -- Member Demographic  
    ,m.member_id
    ,m.gender as member_gender
    ,cast(m.age as int) as member_age

    -- Provider Demographic
    ,pr.Rendering_NPI
    ,pr.Billing_NPI
    ,pr.Taxonomies
    ,pr.Provider_Specialty
    
    --Procedures 
    ,pc.procedure_code
    ,rc.revenue_code  

    -- Placce of Service
    ,pos.Place_of_Service_Code
    ,pos.Place_of_Service_Name
    ,pos.Place_of_Service_Description

    -- Financials    
    ,(fl.line_allowed) as line_allowed
    ,(fl.line_charge) as line_charge
    ,(fl.units) as units
    ,(fl.line_paid) as line_paid

FROM        {gold_fact_claim_header}  f
inner join  {gold_fact_claim_line} fl on f.claim_id = fl.claim_id 
LEFT JOIN   {gold_dim_members} m ON f.member_key = m.member_key
LEFT JOIN   {gold_dim_providers} pr ON f.provider_key = pr.provider_key 
left join   {gold_dim_procedures_codes} pc on fl.procedure_code_key = pc.procedure_code_key
left JOIN   {gold_dim_revenue_codes} rc on fl.revenue_code_key = rc.revenue_code_key
left join   {gold_dim_place_of_service} pos on f.place_of_service_key = pos.place_of_service_key
""")

# Make all columns as proper case
fact_claim_line_view = fact_claim_line_view.select([F.col(x).alias(x.upper()) for x in fact_claim_line_view.columns])

# fact_claim_line_view.createOrReplaceGlobalTempView(f"{dst_catalog}.{dst_schema}.vw_fact_claim_line_enriched") 
#[NOT_SUPPORTED_WITH_SERVERLESS] GLOBAL TEMPORARY VIEW is not supported on serverless compute. SQLSTATE: 0A000

# If you want to persist the view as a table, uncomment the following line:
fact_claim_line_view.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{dst_catalog}.{dst_schema}.fact_claim_line_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14) Basic Validation + Latest Logs

# COMMAND ----------

tables = [    
    gold_dim_members,
    gold_fact_membership,
    gold_dim_providers,
    gold_dim_payers,        
    gold_dim_diagnosis_codes,
    gold_dim_procedures_codes,
    gold_dim_revenue_codes,
    gold_dim_place_of_service,
    gold_dim_date,
    gold_fact_claim_header,
    gold_fact_claim_line,
    gold_mart_payer_month,    
    run_log_table,
    dq_issues_table
]

for t in tables:
    if spark.catalog.tableExists(t):
        print(f"{t}: rows={spark.table(t).count()}")
    else:
        print(f"{t}: (not found)")

print("\nLatest Gold run logs:")
(spark.table(run_log_table)
      .filter(F.col("layer") == "gold")
      .orderBy(F.col("end_time").desc())
      .limit(50)
      .display())

print("\nLatest DQ issues:")
(spark.table(dq_issues_table)
      .orderBy(F.col("issue_timestamp").desc())
      .limit(50)
      .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15) Export final results

# COMMAND ----------

spark.sql("select * from gold_analytics.claims_analytics.dim_members").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/dim_members.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.dim_providers").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/dim_providers.csv", index=False) 
spark.sql("select * from gold_analytics.claims_analytics.dim_procedures_codes").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/dim_procedures_codes.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.dim_diagnosis_codes").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/dim_diagnosis_codes.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.dim_place_of_service").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/dim_place_of_service.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.dim_payers").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/dim_payers.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.dim_revenue_codes").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/dim_revenue_codes.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.dim_date").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/dim_date.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.fact_claim_header").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/fact_claim_header.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.fact_claim_line").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/fact_claim_line.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.bridge_claim_diagnosis").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/bridge_claim_diagnosis.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.fact_claim_header_view").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/fact_claim_header_view.csv", index=False)
spark.sql("select * from gold_analytics.claims_analytics.fact_claim_line_view").toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/fact_claim_line_view.csv", index=False)
spark.table(gold_mart_payer_month).toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/mart_payer_month.csv", index=False)
spark.table(gold_fact_membership).filter(F.year(F.col("year_month_date")) <=2025).toPandas().to_csv("/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/final/fact_membership.csv", index=False)