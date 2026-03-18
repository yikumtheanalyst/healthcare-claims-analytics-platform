use yikumtheanalyst
go


-- TOP Diagnoses Driving Healthcare Spending
select top 5
'TOP Diagnoses Driving Healthcare Spending' as 'Title',
--a.diagnosis_code
--,a.long_description,
e.provider_key,
count(*) as total_rows,
count(distinct b.claim_fact_header_key) as claims
,sum(d.line_charge) as line_charge
,sum(d.line_charge) / count(distinct b.claim_fact_header_key) as Average_line_charge
from diagnosis_codes a
inner join bridge_claim_diagnosis b on a.diagnosis_code_key=b.diagnosis_code_key
inner join claim_header c on b.claim_fact_header_key=c.claim_fact_header_key
inner join claim_lines d on c.claim_fact_header_key =d.claim_fact_header_key
inner join dim_providers e on e.provider_key=c.provider_key
group by
--a.diagnosis_code
--,a.long_description,
e.provider_key

order by line_charge desc;


-- TOP Procedures Driving Healthcare Spending
select top 5
'TOP Procedures Driving Healthcare Spending' as 'Title',
pc.procedure_code,
pc.short_description,
count(*) as total_rows,
count(distinct b.claim_fact_header_key) as claims
,sum(d.line_charge) as line_charge
,sum(d.line_charge) / count(distinct b.claim_fact_header_key) as Average_line_charge
from diagnosis_codes a
inner join bridge_claim_diagnosis b on a.diagnosis_code_key=b.diagnosis_code_key
inner join claim_header c on b.claim_fact_header_key=c.claim_fact_header_key
inner join claim_lines d on c.claim_fact_header_key =d.claim_fact_header_key
inner join [dbo].[dim_procedures_codes] pc on pc.[procedure_code_key] = d.procedure_code_key
group by
pc.procedure_code,
pc.short_description

order by line_charge desc;


-- TOP Average Cost per Claim by Diagnosis
select top 5
'TOP Average Cost per Claim by Diagnosis' as 'Title',
a.diagnosis_code,
a.long_description,
count(*) as total_rows,
count(distinct b.claim_fact_header_key) as claims
,sum(d.line_charge) as line_charge
,sum(d.line_charge) / count(distinct b.claim_fact_header_key) as Average_line_charge
from diagnosis_codes a
inner join bridge_claim_diagnosis b on a.diagnosis_code_key=b.diagnosis_code_key
inner join claim_header c on b.claim_fact_header_key=c.claim_fact_header_key
inner join claim_lines d on c.claim_fact_header_key =d.claim_fact_header_key
group by
a.diagnosis_code
,a.long_description

order by Average_line_charge desc;




-- TOP Average Cost per Claim by Procedures
select top 5
'TOP Procedures Driving Healthcare Spending' as 'Title',
pc.procedure_code,
pc.short_description,
count(*) as total_rows,
count(distinct b.claim_fact_header_key) as claims
,sum(d.line_charge) as line_charge
,sum(d.line_charge) / count(distinct b.claim_fact_header_key) as Average_line_charge
from diagnosis_codes a
inner join bridge_claim_diagnosis b on a.diagnosis_code_key=b.diagnosis_code_key
inner join claim_header c on b.claim_fact_header_key=c.claim_fact_header_key
inner join claim_lines d on c.claim_fact_header_key =d.claim_fact_header_key
inner join [dbo].[dim_procedures_codes] pc on pc.[procedure_code_key] = d.procedure_code_key
group by
pc.procedure_code,
pc.short_description

order by Average_line_charge desc;



select top 5
a.diagnosis_code,
a.long_description,
count(*) as total_rows,
count(distinct b.claim_fact_header_key) as claims
,sum(d.line_charge) as line_charge
,sum(d.line_charge) / count(distinct b.claim_fact_header_key) as Average_line_charge
from diagnosis_codes a
inner join bridge_claim_diagnosis b on a.diagnosis_code_key=b.diagnosis_code_key and b.diagnosis_seq=1
inner join claim_header c on b.claim_fact_header_key=c.claim_fact_header_key
inner join claim_lines d on c.claim_fact_header_key =d.claim_fact_header_key
group by
a.diagnosis_code
,a.long_description

order by line_charge desc;


select top 25
a.diagnosis_code,
a.long_description,
count(*) as total_rows,
count(distinct b.claim_fact_header_key) as claims
,sum(d.line_charge) as line_charge
from diagnosis_codes a
inner join bridge_claim_diagnosis b on a.diagnosis_code_key=b.diagnosis_code_key and b.diagnosis_seq=1
inner join claim_header c on b.claim_fact_header_key=c.claim_fact_header_key
inner join claim_lines d on c.claim_fact_header_key =d.claim_fact_header_key
group by
a.diagnosis_code
,a.long_description

order by claims desc;