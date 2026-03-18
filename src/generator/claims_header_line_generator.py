# Databricks notebook source
# MAGIC %md
# MAGIC # Project Type: Enterprise-Style Synthetic Healthcare Claims Data Platform

# COMMAND ----------

# MAGIC %md
# MAGIC #### Author: Yikum Shiferaw
# MAGIC #### Date: 3/3/2026
# MAGIC #### History:
# MAGIC - 2/28/26  Initial code creation of the Medallion Architecture (bronze-silver-gold) under initial project healthcare-claims-medallion-pipeline
# MAGIC - 3/3/26   The focus shifted from just Medallion Architecture of moving data to designing a more robust, clean, and scalable architecture that mirrors real world healthcare transactions system.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Synthetic Medical Claims Generator (Header + Line Model)
# MAGIC
# MAGIC This notebook generates a **real-world structured** synthetic medical claims dataset:
# MAGIC - `members`
# MAGIC - `claim_header`
# MAGIC - `claim_diagnosis` (bridge: 1..N diagnoses per claim)
# MAGIC - `claim_lines` (1..N procedures per claim line)
# MAGIC
# MAGIC It also includes a **single-patient example** (2025) showing: 1 claim, 5 diagnoses, 1 procedure.
# MAGIC

# COMMAND ----------

import random
import pandas as pd
from datetime import date, timedelta
from collections import Counter

# COMMAND ----------

# MAGIC %md
# MAGIC #### CONFIGURATION

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

# Clear widgets (safe for interactive use; harmless in jobs)
dbutils.widgets.removeAll()

# Optional: reference files location (Unity Catalog Volumes)
dbutils.widgets.text("reference_volume_path", "/Volumes/workspace/default/healthcare_claims_raw/claims_analytics_platform/raw/")

dest_path = dbutils.widgets.get("reference_volume_path")
dest_path

# COMMAND ----------

import os
import openpyxl
from pyspark.sql.functions import substr, col, lit, when, length, concat, trim

raw_file_path = dbutils.widgets.get("reference_volume_path")

df_hcpcs = pd.read_excel(f"{raw_file_path}HCPC2026_JAN_ANWEB_01122026.xlsx") # Procedure Codes
hcpcs_codes = df_hcpcs['HCPC'].dropna().astype(str).str.strip().tolist()

df_pos   = pd.read_csv(f"{raw_file_path}Place of Services.csv", encoding='latin-1')#, sheet_name='Place of Services')
# Rename a column in a pandas DataFrame
df_pos = df_pos.rename(columns={'Place of Service Code(s)': 'Place_of_Service_Code',
                                "Place of Service Name": 'Place_of_Service_Name', 
                                "Place of Service Description": 'Place_of_Service_Description'}
                                )
# Set Place of service code to others when code is not in "11", "21", "22", "23","02","24","31" since the other codes rairly appear in the data
df_pos["Place_of_Service_Code"] = df_pos["Place_of_Service_Code"] \
    .apply(lambda x: "OTHER" if x not in ["11", "21", "22", "23","02","24","31"] else x)

# Extract the first number from the "Place_of_Service_Code" column
pos_codes_raw_list = (
    df_pos["Place_of_Service_Code"]
      .dropna()
      .astype(str)
      .str.extract(r"(\d+)")[0]   # grab first number (handles "73-80")
      .dropna()
      .tolist()
)

if not pos_codes_raw_list:
    raise ValueError("POS extraction failed: pos_codes is empty. Check column name and sheet.")

df_rev = pd.read_excel(f"{raw_file_path}Revenue Codes.xlsx", sheet_name="Sheet1") # Revenue Codes
rev_codes   = df_rev['Revenue Code'].dropna().astype(str).str.strip().tolist()

df_icd10_file = spark.read.text(f"{raw_file_path}icd10cm-codes-2026.txt") # ICD-10-CM Diagnosis Codes
df_icd10_file = df_icd10_file.withColumn("DIAGNOSIS_CODE", trim(col("value").substr(1, 7)))
icd_codes     = df_icd10_file.toPandas()['DIAGNOSIS_CODE'].dropna().astype(str).str.strip().tolist()

# COMMAND ----------

df_pos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Single-Patient Example (2025): 5 Diagnoses, 1 Procedure

# COMMAND ----------

member_id = "MEM000001"
claim_id = "CLM2025000001"

members = pd.DataFrame([{
    "member_id": member_id,
    "gender": "F",
    "birth_date": "1986-04-12",
    "state": "CA",
    "zip3": "10000",
    "payer_type": "Medicaid"
}])

claim_header = pd.DataFrame([{
    "claim_id": claim_id,
    "member_id": member_id,
    "claim_type": "I",
    "payer_type": "Medicaid",
    "service_from_date": "2025-03-18",
    "service_to_date": "2025-03-18",
    "place_of_service_code": "21",
    "billing_npi": "1234567890",
    "rendering_npi": "1098765432"
}])

claim_diagnosis = pd.DataFrame([
    {"claim_id": claim_id, "diagnosis_seq": 1, "diagnosis_code": "E11.9"},
    {"claim_id": claim_id, "diagnosis_seq": 2, "diagnosis_code": "I10"},
    {"claim_id": claim_id, "diagnosis_seq": 3, "diagnosis_code": "E78.5"},
    {"claim_id": claim_id, "diagnosis_seq": 4, "diagnosis_code": "J45.909"},
    {"claim_id": claim_id, "diagnosis_seq": 5, "diagnosis_code": "Z79.4"},
])

line_charge = 850.00
allowed_factor = 0.62

claim_lines = pd.DataFrame([{
    "claim_id": claim_id,
    "service_line_number": 1,
    "procedure_code": "99223",
    "units": 1,
    "revenue_code": "0120",
    "line_charge":  line_charge,
    "line_allowed": round(line_charge * allowed_factor, 2),
    "line_paid":    round(line_charge * allowed_factor, 2)
}])
display(members)
display(claim_header)
display(claim_diagnosis)
display(claim_lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Configuration (Payer Mix, POS, Codes)

# COMMAND ----------

PAYER_TYPES = ["Medicaid", "Commercial", "Medicare", "Unspecified", "Medicare Supplement"]
PAYER_WEIGHTS = [65, 18, 10, 5, 2] # Studies show that medical bills are paid by 65-70% Medicaid, 18-20% Commerial, 9-12% Medicare, 5% Unspecified, and the remaning bills were paid by Medicarre Suplement.

# Claim Types 
CLAIM_TYPES = ["P", "I"] # P = Professional, I = Institutional
CLAIM_TYPE_WEIGHTS = [75, 25]

# Place of Service Codes
POS_CODES   = ["11", "21","22","23","02","24","31","OTHER"] 
#  "Office", "Inpatient", "Outpatient Hospital","ER", "Telehealth","Ambulatory Surgical Center", "Skilled Nursing Facility","Others"
POS_WEIGHTS = [50, 20, 10, 10,3,3,2,2]

ICD10_CODES     = icd_codes#
CPT_CODES       = hcpcs_codes
REVENUE_CODES   = rev_codes

def pick_weighted(values, weights):
    return random.choices(values, weights=weights, k=1)[0]

# Declare total members to use for claim activities generation


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) Helper functions

# COMMAND ----------

import random
import pandas as pd

specialty = [
     'Cardiologist',
    'Psychiatrist',
     'Psychologist',       
    'Therapist',
     'Dermatologist',
     'Endocrinologist',
     'Gastroenterologist',
     'Geriatrician',
     'Hematologist',
     'Infectious Disease Specialist',
     'Nephrologist',
     'Neurologist',
     'Obstetrician',
     'Oncologist',
     'Ophthalmologist',
     'Orthopedic Surgeon',
     'Pain Management Specialist',
     'Pulmonologist',
     'Rheumatologist',     
     'Allergist',
     'Anesthesiologist',
     'Bariatric Surgeon',
     'Psychiatric Nurse',
     'Psychiatric Social Worker',       
     'Speech-Language Pathologist',
     'Eumunologist',
     'Nurse Practitioner'    
]

taxonomies = [
    '207Q00000X',
    '207R00000X',
    '207RC0000X',
    '207X00000X',
    '207RG0100X',
    '207RX0202X',
    '207P00000X',
    '208000000X',
    '208100000X',
    '208200000X',
    '208300000X',
    '208400000X',
    '208500000X',
    '208600000X',
    '208700000X',
    '208800000X',
]

rendering_npi       = [str(random.randint(1000000000, 9999999999)) for _ in range(500)]
billing_npi         =   [str(random.randint(1000000000, 9999999999)) for _ in range(500)]
taxonomies_types    = [random.choice(taxonomies) for _ in rendering_npi]
provider_specialty = [random.choice(specialty) for _ in rendering_npi]

providers_pdf = pd.DataFrame({
    "rendering_npi": rendering_npi,
    "billing_npi": billing_npi,
    "taxonomies": taxonomies_types,
    "provider_specialty": provider_specialty
})

# display(providers_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Build Enrollment Eligibility and Monthly Membership
# MAGIC #### Enrollment table holds attribues that do not change for a member often, such as name, gender, dob, etc.
# MAGIC #### Membership is a time-dependent, not a static demographic attribue.  

# COMMAND ----------

from datetime import date, timedelta
from pyspark.sql import functions  as F


def random_birthdate():
    year = random.randint(1940, 2025)
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))
        
def generate_members(n_members: int, year: int) -> pd.DataFrame:
    states = [  'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ',
              'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
    genders = ["M","F","U"]
    rows = []
    duration_days   = random.randint(60, 365)
    for i in range(1, n_members + 1):
        payer = pick_weighted(PAYER_TYPES, PAYER_WEIGHTS)
        birth_year = random.randint(1940, 2022)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 31)
        eligibility_start_date = date((birth_year+ random.randint(1, 10)),random.randint(1, 12),random.randint(1, 28)) + \
                                                                                                                                                                                                               timedelta(days=duration_days)
        eligibility_end_date = eligibility_start_date+ timedelta(days=duration_days)
        rows.append({
            "member_id": f"MEM{i:06d}",
            "gender": random.choice(genders),
           "birth_date": random_birthdate().strftime("%Y-%m-%d"),
            "state": random.choice(states),
            "zip3": str(random.randint(99990, 99996)),
            "payer_type": payer
        })
    return pd.DataFrame(rows)


total_members=11368
enrollment_df_2024   = generate_members(total_members, year=2024)
total_members=9489
enrollment_df_2025   = generate_members(total_members, year=2025)

enrollment_df   = pd.concat([enrollment_df_2024, enrollment_df_2025])

def random_eligibility_birthdate():
    year = random.randint(2024, 2025)
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

# Add start and end eligbility dates to enrollment_df
duration_days   = random.randint(60, 365)
enrollment_df["eligibility_start_date"] = enrollment_df.apply(lambda x: random_eligibility_birthdate(), axis=1)
enrollment_df["eligibility_end_date"]   = enrollment_df["eligibility_start_date"] + timedelta(days=duration_days)

enrollment_df["enroll_year"] =  pd.to_datetime(enrollment_df["eligibility_start_date"]).dt.year

enrollment_df.groupby(["payer_type","enroll_year"])['member_id'].count().reset_index()

from datetime import date, timedelta
from pyspark.sql import functions  as F

# Convert Pandas DataFrame to Spark DataFrame
enrollment_sp_df = spark.createDataFrame(enrollment_df)

# Ensure dates types
enrollment_sp_df = enrollment_sp_df.withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))
enrollment_sp_df = enrollment_sp_df.withColumn("eligibility_start_date", F.to_date(F.col("eligibility_start_date"), "yyyy-MM-dd"))
enrollment_sp_df = enrollment_sp_df.withColumn("eligibility_end_date", F.to_date(F.col("eligibility_end_date"), "yyyy-MM-dd"))

# generate monthly sequence of dates for each member
enrollment_month_df = enrollment_sp_df.withColumn("month_sequence", F.expr("sequence(eligibility_start_date, eligibility_end_date, interval 1 month)"))

# Expanda into rows
membership_month_df = enrollment_month_df.select("*", F.explode("month_sequence").alias("month_start"))

# Add year/month/first day columns
membership_month_df = membership_month_df.withColumn("year", F.year("month_start"))
membership_month_df = membership_month_df.withColumn("month", F.when(F.month(F.col("month_start")) <10, F.concat(F.lit('0'),F.month(F.col("month_start"))).cast('string')).otherwise(F.month(F.col("month_start")).cast("string")))
membership_month_df = membership_month_df.withColumn("day",F.lit('01'))

# Reset fture enrollment of back to 2025 because members could enroll in 2025 and their termination date >=2025
# membership_month_df_sp = membership_month_df
membership_month_df = membership_month_df.withColumn("eligibility_end_date", F.when(F.col("year") > 2025, F.lit(date(2025, 12, 31))).otherwise(F.col("eligibility_end_date")))
membership_month_df = membership_month_df.withColumn("year", F.when(F.col("year") > 2025, F.lit(2025)).otherwise(F.col("year")))
# membership_month_df_sp = membership_month_df_sp.withColumn("year", F.when(F.col("year") > 2025, F.col('year')).otherwise(F.col("year")))

# # Add year/month/first day columns
# membership_month_df_sp = membership_month_df_sp.withColumn("year", F.["eligibility_end_date"] = F.when(F.col("year") > 2025, F.lit(date(2025, 12, 31))).otherwise(F.col("eligibility_end_date"))

# # Year should also be set back t 2025
# membership_month_df["year"] = F.when(F.col("year") == 2025, F.lit(2025)).otherwise(F.col("year"))

# # Filte for 2024 and 2025
membership_month_df = membership_month_df.withColumn("Date_ID", F.concat(F.col("year"),F.lit("-"),F.col("month"),F.lit("-"),F.col("day")))
membership_month_df = membership_month_df.withColumn("Date_ID", F.to_date(F.col("Date_ID"), "yyyy-MM-dd"))
membership_month_df = membership_month_df.withColumn("member_count", F.lit(1))

# membership_month_final_df = membership_month_df.filter((F.col("year") >= 2024) & (F.col("year") <= 2025)).toPandas()

membership_month_df = membership_month_df.drop("month_sequence","month_start","month","day")
membership_month_final_df = membership_month_df.toPandas()

membership=membership_month_final_df.groupby(["payer_type","enroll_year"]).agg(membership=('member_count', 'sum')).reset_index()
# display(membership.sort_values(["payer_type","enroll_year"], ascending=[False,False]))

# COMMAND ----------

from datetime import date, timedelta

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def random_birthdate():
    year = random.randint(1940, 2025)
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def random_eligibility_date():
    year = random.randint(1940, 2025)
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    delta = (end - start).days
    return pd.to_datetime(start + timedelta(days=random.randint(0, delta)))

def generate_claims(enrollment_df: pd.DataFrame,
                    claims_per_member_min=1,
                    claims_per_member_max=3,
                    max_lines_per_claim=3,
                    max_dx_per_claim=4,
                    year=None):
    start = date(year,1,1)
    end = date(year,12,31)
    
    claim_header_rows = []
    claim_dx_rows = []
    claim_line_rows = []
    
    claim_counter = 1
    
    rendering_npi, billing_npi, provider_specialty,taxonomies_id = random.choice(list(providers_pdf.itertuples(index=False, name=None)))
    for _, m in enrollment_df.iterrows():
        n_claims = random.randint(claims_per_member_min, claims_per_member_max)
        for _ in range(n_claims):
            cid = f"CLM{year}{claim_counter:07d}"
            claim_counter += 1
            
            ctype = pick_weighted(CLAIM_TYPES, CLAIM_TYPE_WEIGHTS)
            payer = m["payer_type"]
            pos = pick_weighted(POS_CODES, POS_WEIGHTS)
            svc_date = random_date(start, end)
            
            claim_header_rows.append({
                "claim_id": cid,
                "member_id": m["member_id"],
                "claim_type": ctype,
                "payer_type": payer,
                "service_from_date": svc_date.isoformat(),
                "service_to_date": svc_date.isoformat(),
                "place_of_service_code": pos,
                "billing_npi": billing_npi , #str(random.randint(1000000000, 9999999999)),
                "rendering_npi":rendering_npi, # str(random.randint(1000000000, 9999999999)),
                "provider_specialty":provider_specialty,
                "taxonomies_id":taxonomies_id,
                "year":year
            })
            
            n_dx = random.randint(1, max_dx_per_claim)
            dx_codes = random.sample(ICD10_CODES, k=min(n_dx, len(ICD10_CODES)))
            for seq, dx in enumerate(dx_codes, start=1):
                claim_dx_rows.append({
                    "claim_id": cid,
                    "diagnosis_seq": seq,
                    "diagnosis_code": dx
                })
            
            n_lines = random.randint(1, max_lines_per_claim)
            for line_no in range(1, n_lines+1):
                proc = random.choice(CPT_CODES)
                units = random.randint(1, 3)
                
                base = random.uniform(80, 250) if ctype == "P" else random.uniform(300, 1500)
                charge = round(base * units, 2)
                
                factor = {
                    "Medicaid": 0.60,
                    "Medicare": 0.75,
                    "Commercial": 0.85,
                    "Medicare Supplement": 0.80,
                    "Unspecified": 0.65
                }.get(payer, 0.70)
                
                allowed = round(charge * factor, 2)
                paid = allowed
                
                rev = random.choice(REVENUE_CODES) if ctype == "I" else None
                
                claim_line_rows.append({
                    "claim_id": cid,
                    "service_line_number": line_no,
                    "procedure_code": proc,
                    "units": units,
                    "revenue_code": rev,
                    "line_charge": charge,
                    "line_allowed": allowed,
                    "line_paid": paid
                })
    
    return pd.DataFrame(claim_header_rows), pd.DataFrame(claim_dx_rows), pd.DataFrame(claim_line_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) 2025 Claims Generator (Members, Claims, Lines, Diagnoses, Place of Service)

# COMMAND ----------

payer_utilization_rate = [("Commercial", 0.75), ("Medicaid", 0.35), ("Medicare", 0.75), ("Medicare Supplement", 0.81), ("Unspecified", 0.43)]
claims_header_df_list   = pd.DataFrame()
icd10_diagnosis_df_list = pd.DataFrame()
claims_line_df_list     = pd.DataFrame()

selected_sample_list =[]
sample_frac= 0.35
sampled_members_df = enrollment_df.sample(frac=sample_frac)
selected_sample_list.append(sampled_members_df)

utilizing_members = pd.concat(selected_sample_list)
claims_header_df, icd10_diagnosis_df, claims_line_df    = generate_claims(utilizing_members, claims_per_member_min=1, claims_per_member_max=2, year=2024)

claims_header_df_list = pd.concat([claims_header_df_list,claims_header_df])
icd10_diagnosis_df_list = pd.concat([icd10_diagnosis_df_list,icd10_diagnosis_df])
claims_line_df_list     = pd.concat([claims_line_df_list,claims_line_df])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) 2025 Claims Generator (Members, Claims, Lines, Diagnoses, Place of Service)

# COMMAND ----------

selected_sample_list =[]
sample_frac= 0.55
sampled_members_df = enrollment_df.sample(frac=sample_frac)
selected_sample_list.append(sampled_members_df)

utilizing_members = pd.concat(selected_sample_list)
claims_header_df, icd10_diagnosis_df, claims_line_df    = generate_claims(utilizing_members, claims_per_member_min=1, claims_per_member_max=2, year=2025)

claims_header_df_list = pd.concat([claims_header_df_list,claims_header_df])
icd10_diagnosis_df_list = pd.concat([icd10_diagnosis_df_list,icd10_diagnosis_df])
claims_line_df_list     = pd.concat([claims_line_df_list,claims_line_df])

# COMMAND ----------

membership=membership_month_final_df.groupby(["payer_type","enroll_year"]).agg(membership=('member_count', 'sum')).reset_index()
utilization = claims_header_df_list.groupby(["payer_type","year"]).agg(utilization=('member_id', 'nunique')).reset_index()

# display(membership.sort_values(["payer_type","enroll_year"], ascending=False))
# display(utilization.sort_values(["payer_type","year"], ascending=False))

membership_month_final_df.groupby(["payer_type","enroll_year"]).agg(membership=('member_count', 'sum')).reset_index()
utilization_df = claims_header_df_list.groupby(["payer_type","year"]).agg(utilization=('member_id', 'nunique')).reset_index()

membership_month_utilization_final  =spark.createDataFrame(membership).withColumnRenamed("enroll_year","year").join(spark.createDataFrame(utilization), on=["payer_type","year"])
display(membership_month_utilization_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Do a quick data validation by picking random members from the final data

# COMMAND ----------

id = 'MEM001958'
display(enrollment_df[enrollment_df["member_id"]==id])
display(claims_header_df_list[claims_header_df_list["member_id"]==id])
display(icd10_diagnosis_df_list[icd10_diagnosis_df_list["claim_id"].isin(claims_header_df_list[claims_header_df_list["member_id"]==id]["claim_id"])])
display(claims_line_df_list[claims_line_df_list["claim_id"].isin(claims_header_df_list[claims_header_df_list["member_id"]==id]["claim_id"])])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Export CSVs (workspace/ volumes)

# COMMAND ----------

# DBTITLE 1,Export Files
enrollment_df.to_csv(f"{dest_path}members.csv", index=False)
membership_month_final_df.to_csv(f"{dest_path}membership_month.csv", index=False)
claims_header_df_list.to_csv(f"{dest_path}claims_header.csv", index=False)
icd10_diagnosis_df_list.to_csv(f"{dest_path}claims_diagnosis.csv", index=False)
claims_line_df_list.to_csv(f"{dest_path}claims_lines.csv", index=False)
providers_pdf.to_csv(f"{dest_path}providers.csv", index=False)

print("Wrote: members.csv,membership_month.csv,claims_header.csv, claims_diagnosis.csv, claims_lines.csv, providers.csv")