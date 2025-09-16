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

{# with
  -- measures + källfält
  ja as (
    select *
    from {{ ref('src_job_ads') }}
  ),

  -- arbetsplatsfält direkt från src_employer
  je as (
    select id, workplace_region, workplace_postcode, workplace_street_address
    from {{ ref('src_employer') }}
  ),

  -- mapping-CTE:er för att bygga samma SK som dimmarna
  jdm as (
    select id, headline
    from {{ ref('src_job_details') }}
  ),
  em as (
    select id, employer_name
    from {{ ref('src_employer') }}
  ),

  -- dimensioner
  jd as (select * from {{ ref('dim_job_details') }}),
  e  as (select * from {{ ref('dim_employer') }}),
  a  as (select * from {{ ref('dim_auxilliary_attributes') }}),
  o  as (select * from {{ ref('dim_occupation') }})

select
  -- FK från dimensionerna
  o.occupation_id,
  jd.job_details_id,
  e.employer_id,
  a.auxilliary_attributes_id,

  -- measures från ads
  ja.vacancies,
  ja.relevance,
  ja.application_deadline,

  -- attribut
  jd.headline,
  jd.employment_type,
  jd.description,
  e.employer_name,
  e.employer_organization_number,
  o.occupation,
  a.experience_required,
  je.workplace_region,
  je.workplace_postcode,
  je.workplace_street_address
from ja
-- job_details via samma SK: hash(id, headline)
left join jdm on jdm.id = ja.id
left join jd  on jd.job_details_id = {{ dbt_utils.generate_surrogate_key(['jdm.id','jdm.headline']) }}

-- employer via samma SK: hash(id, employer_name)
left join em on em.id = ja.id
left join e  on e.employer_id = {{ dbt_utils.generate_surrogate_key(['em.id','em.employer_name']) }}

-- aux via samma SK: hash(id)
left join a  on a.auxilliary_attributes_id = {{ dbt_utils.generate_surrogate_key(['ja.id']) }}

-- occupation via samma SK: hash(occupation)
left join o  on o.occupation_id = {{ dbt_utils.generate_surrogate_key(['ja.occupation__label']) }}

-- arbetsplatsfält
left join je on je.id = ja.id
 #}


 with
  -- measures + källfält
  ja as (
    select *
    from {{ ref('src_job_ads') }}
  ),

  -- arbetsplatsfält direkt från src_employer
  je as (
    select id, workplace_region, workplace_postcode, workplace_street_address
    from {{ ref('src_employer') }}
  ),

  -- mapping-CTE:er för att bygga samma SK som dimmarna
  jdm as (
    select id, headline
    from {{ ref('src_job_details') }}
  ),
  em as (
    select id, employer_name
    from {{ ref('src_employer') }}
  ),

  -- dimensioner
  jd as (select * from {{ ref('dim_job_details') }}),
  e  as (select * from {{ ref('dim_employer') }}),
  a  as (select * from {{ ref('dim_auxilliary_attributes') }}),
  o  as (select * from {{ ref('dim_occupation') }})

select
  -- FK från dimensionerna
  o.occupation_id,
  jd.job_details_id,
  e.employer_id,
  a.auxilliary_attributes_id,

  -- measures från ads
  ja.vacancies,
  ja.relevance,
  ja.application_deadline,

  -- attribut
  jd.headline,
  jd.employment_type,
  jd.description,
  e.employer_name,
  e.employer_organization_number,
  o.occupation,
  a.experience_required,
  je.workplace_region,
  je.workplace_postcode,
  je.workplace_street_address,
  a.auxilliary_attributes_id,
  jd.job_details_id,


from ja
-- job_details via samma SK: hash(id, headline)
left join jdm on jdm.id = ja.id
left join jd  on jd.job_details_id = {{ dbt_utils.generate_surrogate_key(['jdm.id','jdm.headline']) }}

-- employer via samma SK: hash(id, employer_name)
left join em on em.id = ja.id
left join e  on e.employer_id = {{ dbt_utils.generate_surrogate_key(['em.id','em.employer_name']) }}

-- aux via samma SK: hash(id)
left join a  on a.auxilliary_attributes_id = {{ dbt_utils.generate_surrogate_key(['ja.id']) }}

-- occupation via samma SK: hash(occupation)
left join o  on o.occupation_id = {{ dbt_utils.generate_surrogate_key(['ja.occupation__label']) }}

-- arbetsplatsfält
left join je on je.id = ja.id

{# where e.employer_name = 'Eterni Sweden AB' AND je.workplace_region = 'Kronobergs län' #}


