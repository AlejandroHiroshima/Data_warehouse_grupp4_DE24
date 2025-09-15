{# with
  ja as (select * from {{ ref('src_job_ads') }}),                 -- id, occupation__label, vacancies, relevance, application_deadline
  jd as (select * from {{ ref('dim_job_details') }}),             -- job_details_id, id, headline, description, employment_type, ...
  e  as (select * from {{ ref('dim_employer') }}),                -- employer_id, id, employer_name, employer_organization_number, ...
  a  as (select * from {{ ref('dim_auxilliary_attributes') }}),   -- auxilliary_attributes_id, id, experience_required, ...
  o  as (select * from {{ ref('dim_occupation') }})               -- occupation_id, occupation, occupation_group, occupation_field

select
  -- använd färdiga SK direkt från dimensionerna
  o.occupation_id,
  jd.job_details_id,
  e.employer_id,
  a.auxilliary_attributes_id,

  -- mätvärden/attribut i facten
  ja.vacancies,
  ja.relevance,
  ja.application_deadline,
  jd.employment_type,
  o.occupation,
  a.experience_required,
  jd.headline,
  e.employer_organization_number,
  e.employer_name,
  jd.description

from ja
left join jd on jd.job_details_id = ja.id                 -- dim_job_details bär original-id: joina på det
left join e  on e.employer_idid  = ja.id                 -- dim_employer bär original-id: joina på det
left join a  on a.auxilliary_attributes_id  = ja.id                 -- dim_auxilliary_attributes bär original-id: joina på det
left join o  on o.occupation = ja.occupation__label  -- matcha mot textetiketten #}


------ 
{# 
with
  ja as (select * from {{ ref('src_job_ads') }}),            -- id, headline, occupation__label, vacancies, relevance, application_deadline  :contentReference[oaicite:0]{index=0}
  jd as (select * from {{ ref('src_job_details') }}),         -- id, headline, ...                                                :contentReference[oaicite:1]{index=1}
  e  as (select * from {{ ref('src_employer') }}),            -- id, employer_name, ...                                          :contentReference[oaicite:2]{index=2}
  a  as (select * from {{ ref('src_auxilliary_attributes') }}), -- id, experience_required, driving_license_required, access_to_own_car :contentReference[oaicite:3]{index=3}
  o  as (select * from {{ ref('src_occupation') }})           -- occupation (label), group, field                                 :contentReference[oaicite:4]{index=4}

select
  -- FK:er som matchar era dimensioners surrogate keys
  {{ dbt_utils.generate_surrogate_key(['o.occupation']) }}          as occupation_id,
  {{ dbt_utils.generate_surrogate_key(['jd.id','jd.headline']) }}   as job_details_id,
  {{ dbt_utils.generate_surrogate_key(['e.employer_name']) }}       as employer_id,
  {{ dbt_utils.generate_surrogate_key(['a.id']) }}                  as auxilliary_attributes_id,

  -- Mätvärden/attribut i fact-tabellen
  ja.vacancies,
  ja.relevance,
  ja.application_deadline

from ja
left join jd on jd.id = ja.id
left join e  on e.id  = ja.id
left join a  on a.id  = ja.id
left join o  on o.occupation = ja.occupation__label
; #}

---- test med dim




