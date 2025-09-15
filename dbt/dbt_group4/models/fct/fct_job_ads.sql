-- with job_ads as (select * from) {{ ref('src_job_ads') }}

-- select
--     {{ dbt_utils.generate_surrogate_key (['occupation__label']) }} as occupation_id,
--     vacancies,
--     relevance,
--     applcation_deadline

-- from job_ads


-- Debbies

{# with job_ads as (

    select *
    from {{ ref('src_job_ads') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['occupation__label']) }} as occupation_id,
    vacancies,
    relevance,
    application_deadline
from job_ads #}

--- Kuckhans--- 

{# with ja as (select * from {{ ref('src_job_ads') }}),

jd as (select * from {{ ref('src_job_details') }}),

e as (select * from {{ ref('src_employer') }})

select
    {{ dbt_utils.generate_surrogate_key(['jd.id', 'jd.headline']) }} as job_details_key,
    {{ dbt_utils.generate_surrogate_key(['jd.id', 'e.employer_name']) }} as employer_key,

    vacancies,
    relevance,
    application_deadline,

    e.employer_name,
    jd.description #}


---- test
{# 
with ja as (select * from {{ ref('src_job_ads') }}),

o as (select * from {{ ref('src_occupation') }}),

jd as (select * from {{ ref('src_job_details') }}),

e as (select * from {{ ref('src_employer') }})

a as (select * from {{ ref('src_auxilliary_attributes.sql') }})

select
    {{ dbt_utils.generate_surrogate_key(['o.id', 'o.occupation']) }} occupation_id,
    {{ dbt_utils.generate_surrogate_key(['jd.id', 'jd.headline']) }} as job_details_id,
    {{ dbt_utils.generate_surrogate_key(['e.id', 'e.employer_name']) }} as employer_id,
    {{ dbt_utils.generate_surrogate_key(['a.id', 'a.experience_required']) }} as auxilliary_attributes_id,


    vacancies,
    relevance,
    application_deadline,

    e.employer_name,
    jd.description


    
from 
    ja
left join
    ja ON o.id = ja.id
left join
    e ON o.id = e.id
left join
    a on a.id = e.id #}


with
  ja as (select * from {{ ref('src_job_ads') }}),                 
  jd as (select * from {{ ref('src_job_details') }}),             
  e  as (select * from {{ ref('src_employer') }}),                
  a  as (select * from {{ ref('src_auxilliary_attributes') }}),   
  o  as (                                                          
    select distinct occupation, occupation_group, occupation_field
    from {{ ref('src_occupation') }}
  )

select
  -- surrogate keys 
  {{ dbt_utils.generate_surrogate_key(['o.occupation']) }}         as occupation_id,
  {{ dbt_utils.generate_surrogate_key(['jd.id', 'jd.headline']) }} as job_details_id,
  {{ dbt_utils.generate_surrogate_key(['e.employer_name']) }}      as employer_id,
  {{ dbt_utils.generate_surrogate_key(['a.id']) }}                 as auxilliary_attributes_id,

  -- attribut
  ja.vacancies,
  ja.relevance,
  ja.application_deadline,
  e.employer_name,
  jd.headline,
  jd.description

from ja
left join jd on jd.id = ja.id
left join e  on e.id  = ja.id
left join a  on a.id  = ja.id
left join o  on o.occupation = ja.occupation__label;
