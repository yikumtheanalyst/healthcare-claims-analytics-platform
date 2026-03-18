
# Healthcare Claims Analytics Platform

End‑to‑end **Healthcare Claims Analytics Platform** built using **Databricks, PySpark, Delta Lake, Medallion Architecture, Dimensional Modeling, and Power BI**.

This project demonstrates how a modern healthcare analytics platform ingests, transforms, models, validates, and visualizes medical claims data using industry‑standard architecture patterns.

---

# Project Objective

Healthcare organizations process massive volumes of claims data involving:

• Members (patients)  
• Providers  
• Diagnoses (ICD‑10)  
• Procedures (HCPCS/CPT)  
• Revenue codes  
• Place of service  
• Insurance payers  

This project simulates a **real healthcare analytics platform**, transforming raw claims data into a **clean, scalable analytics model** used for reporting and decision‑making.

The platform follows a **Medallion Architecture pipeline**:

```
Raw Data → Bronze → Silver → Gold → Analytics / BI
```

---

# Architecture Overview

Below is the logical architecture of the platform.

```
                Raw Healthcare Data
                       │
                       ▼
                Bronze Layer
         Raw ingestion into Delta tables
                       │
                       ▼
                Silver Layer
        Data cleansing and normalization
                       │
                       ▼
                 Gold Layer
        Dimensional analytics data model
                       │
                       ▼
                 Power BI
        Executive dashboards & analytics
```

### Architecture Diagram

(Add image to docs folder)

```
docs/architecture_diagram.png
```

Example preview in README:

![Architecture Diagram](docs/architecture_diagram.png)

---

# Medallion Pipeline

## Bronze Layer — Raw Ingestion

Purpose:

• Ingest raw source data  
• Preserve original structure  
• Minimal transformation  
• Store in Delta Lake tables

Example tables:

```
bronze_analytics.claims_analytics.claims_header
bronze_analytics.claims_analytics.claims_lines
bronze_analytics.claims_analytics.claims_diagnosis
```

Characteristics:

• Raw schema preserved  
• Data lineage maintained  
• Source auditing enabled

---

## Silver Layer — Cleaned Data

Purpose:

• Data cleansing  
• Type casting  
• Standardized schemas  
• Normalized relationships

Example tables:

```
silver_analytics.claims_analytics.claims_header
silver_analytics.claims_analytics.claims_lines
silver_analytics.claims_analytics.claims_diagnosis
```

Transformations performed:

• Data type normalization  
• Column standardization  
• Deduplication  
• Relationship preparation

---

## Gold Layer — Analytics Model

Purpose:

Create a **dimensional star schema** optimized for analytics and BI.

Gold tables include:

```
gold_analytics.claims_analytics.fact_claim_header
gold_analytics.claims_analytics.fact_claim_line
gold_analytics.claims_analytics.bridge_claim_diagnosis

gold_analytics.claims_analytics.dim_members
gold_analytics.claims_analytics.dim_providers
gold_analytics.claims_analytics.dim_diagnosis_codes
gold_analytics.claims_analytics.dim_procedure_codes
gold_analytics.claims_analytics.dim_revenue_codes
gold_analytics.claims_analytics.dim_place_of_service
gold_analytics.claims_analytics.dim_payers
```

---

# Data Model

The analytics layer follows a **Healthcare Claims Star Schema**.

### Fact Tables

#### fact_claim_header

One row per claim.

Key columns:

• claim_fact_header_key  
• claim_id  
• member_key  
• provider_key  
• payer_key  
• date_key  
• service_from_date  
• service_to_date  

---

#### fact_claim_line

One row per claim service line.

Key columns:

• claim_fact_line_key  
• claim_id  
• service_line_number  
• procedure_key  
• revenue_code_key  
• pos_key  
• line_charge  
• line_allowed  
• line_paid  

---

### Bridge Table

#### bridge_claim_diagnosis

Claims may have multiple diagnoses.

The bridge table maintains relationships without duplicating financial values.

Columns:

• claim_fact_header_key  
• diagnosis_code_key  
• diagnosis_sequence  

This prevents **financial duplication when multiple diagnoses exist**.

---

### Dimension Tables

• dim_members
• dim_providers
• dim_diagnosis_codes  
• dim_procedure_codes  
• dim_revenue_codes  
• dim_place_of_service
• dim_payers  

---

# Data Model Diagram

Add diagram here:

```
docs/data_model_diagram.png
```

Preview:

![Data Model Diagram](docs/data_model_diagram.png)

---

# Data Quality Validation

The pipeline includes validation checks to guarantee **data integrity across layers**.

### Row Count Validation

| Table | Bronze | Silver | Gold |
|------|------|------|------|
Claims Header | 7557 | 7557 | 7557 |
Claims Lines | 15191 | 15191 | 15191 |
Diagnosis Bridge | 18941 | 18941 | 18941 |

These checks ensure:

• No data loss  
• No accidental duplication  
• Consistent transformation results

---

### Financial Reconciliation

Financial totals were validated across layers.

```
SUM(line_charge)
SUM(line_allowed)
SUM(line_paid)
```

All totals matched exactly between Bronze, Silver, and Gold tables.

Example validation result:

| Metric | Value |
|------|------|
Claim Lines | 15191 |
Total Charges | 10,430,231 |
Total Allowed | 6,956,322 |
Total Paid | 6,956,322 |

---

### Validation Report

Full validation results can be exported as HTML:

```
docs/data_validation_report.html
```

---

# Power BI Analytics

The Gold layer powers executive dashboards.

### Example Analytics

Executive KPIs:

• Total Claims  
• Total Charges  
• Total Paid Amount  
• Allowed Amount  
• Claims per Member  

Operational insights:

• Claims by diagnosis  
• Claims by procedure  
• Provider utilization  
• Payer distribution  
• Place‑of‑service analysis  

---

# Power BI Dashboard Preview

```
docs/Power BI_Tableau Dashboard_Reporting.docx
```

Example:

![Power BI Dashboard](Power BI_Tableau Dashboard_Reporting.docx)

---

# Repository Structure

```
healthcare-claims-analytics-platform
│
├── notebooks
│   ├── 02_healthcare_claims_analytics_bronze.py
│   ├── 03_healthcare_claims_analytics_silver.py
│   └── 04_healthcare_claims_analytics_gold.py
│
├── data
│   └── synthetic_claims_sample.csv
│
├── docs
│   ├── architecture_diagram.png
│   ├── data_model_diagram.png
│   ├── powerbi_dashboard_preview.png
│   └── data_validation_report.html
│
├── powerbi
│   └── healthcare_claims_dashboard.pbix
│
└── README.md
└── requirements.txt
```

---

# Technologies Used

• Databricks  
• PySpark  
• Delta Lake  
• Medallion Architecture  
• Dimensional Modeling  
• SQL  
• Power BI  
• Healthcare Data Modeling  

---

# Key Engineering Concepts Demonstrated

• Medallion data architecture  
• Healthcare claims modeling  
• Fact / dimension design  
• Bridge tables for many‑to‑many relationships  
• Data pipeline validation  
• Financial reconciliation checks  
• Analytics‑ready data modeling  

---

# Future Enhancements

Possible extensions:

• Claims adjudication simulation  
• Denial analytics pipeline  
• Provider performance metrics  
• Risk adjustment modeling  
• Real‑time streaming claims ingestion  
• ML‑based cost prediction

---

# Author
Yikum Shiferaw
Developed as part of a professional **Data Engineering & Analytics portfolio** demonstrating healthcare domain expertise and modern analytics platform design.
