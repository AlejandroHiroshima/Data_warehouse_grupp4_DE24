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



--- SRC


{# with
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
  jd.headline,
  jd.employment_type,
  e.employer_organization_number,
  o.occupation,
  jd.description,
  a.experience_required,
  e.employer_name,
  e.workplace_region,
  e.workplace_postcode,
  e.workplace_street_address,
  a.id,
  jd.id,



from ja
left join jd on jd.id = ja.id
left join e  on e.id  = ja.id
left join a  on a.id  = ja.id
left join o  on o.occupation = ja.occupation__label
where employer_name = 'Eterni Sweden AB' AND workplace_region = 'Kronobergs län'; #}


--- fct från dim.


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

  -- attribut
  o.occupation_id,
  jd.job_details_id,
  e.employer_id,
  a.auxilliary_attributes_id,
  ja.vacancies,
  ja.relevance,
  ja.application_deadline,
  
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
