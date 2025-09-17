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
left join je on je.id = ja.id }


with job_ads as (

    select *
    from {{ ref('src_job_ads') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['occupation__label']) }} as occupation_id,
    vacancies,
    relevance,
    application_deadline


#}
--- Eyoubs 

{# with fct_job_ads as (select * from {{ ref('src_job_ads') }})
 
select
    {{ dbt_utils.generate_surrogate_key(['occupation__label']) }} as occupation_id,
    {{ dbt_utils.generate_surrogate_key(['job_details_id']) }} as job_details_id,
    {{ dbt_utils.generate_surrogate_key(['employer_id']) }} as employer_id,
    {{ dbt_utils.generate_surrogate_key(['auxilliary_attributes_id']) }} as auxilliary_attributes_id,
    vacancies,
    relevance,
    application_deadline
from fct_job_ads #}

-- Alexs

{# with ja as (select * from {{ ref('src_job_ads') }}),

-- hämta surrogates

dim_occupation as (select * from {{ ref('dim_occupation') }}),
dim_job_details as (select * from {{ ref('dim_job_details') }}),
dim_employer as (select * from {{ ref('dim_employer') }}),
dim_auxilliary_attributes as (select * from {{ ref('dim_auxilliary_attributes ') }}),

-- matcha src data mot dimensioner

src_job_details as (select * from {{ ref('src_job_details') }}),
src_employer as (select * from {{ ref('src_employer') }}),
src_auxilliary_attributes as (select * from {{ ref('src_auxilliary_attributes') }}),
src_occupation as (select * from {{ ref('src_occupation') }})

-- select

select

ja.id as job_ad_id 

-- Foreign keys

dim_occupation.occupation_id,
dim_job_details.job_details_id,
dim_employer.employer_id,
dim_auxilliary_attributes.auxilliary_attributes_id,

-- övriga attributes

ja.vacancies,
ja.relevance,
ja.applcation_deadline

from ja

-- The joins
 
left join src_occupation on src_occupation.id = ja.id
left join src_job_details on src_job_details.id = ja.id
left join src_employer on src_employer.id = ja.id
left join src_auxilliary_attributes on src_auxilliary_attributes.id = ja.id

-- Join mot dimensioner för att få surrogate keys  
left join dim_occupation on dim_occupation.occupation = src_occupation.occupation  
left join dim_job_details on 
  (dim_job_details.employment_type = src_job_details.employment_type 
  and dim_job_details.duration = src_job_details.duration     
  and dim_job_details.salary_type = src_job_details.salary_type 
  and dim_job_details.scope_of_work_min = src_job_details.scope_of_work_min 
  and dim_job_details.scope_of_work_max = src_job_details.scope_of_work_max )

left join dim_employer on 
(dim_employer.employer_name = src_employer.employer_name      
and dim_employer.employer_organization_number = src_employer.employer_organization_number     
and dim_employer.workplace_city = src_employer.workplace_city
and dim_employer.workplace_street_address = src_employer.workplace_street_address
and dim_employer.workplace_postcode = src_employer.workplace_postcode
and dim_employer.employer_w

)

-- lägg till fler fält enligt er gruppering i dim_employer  )  

left join dim_auxiliary on (
  dim_auxiliary.experience_required = src_auxiliary.experience_required 
  and dim_auxiliary.driver_license = src_auxiliary.driver_license 
  and dim_auxiliary.access_to_own_car = src_auxiliary.access_to_own_car 
  ) #} 
 

  {# with
  -- measures + källfält
  ja as (
    select *
    from {{ ref('src_job_ads') }}
  ),

  -- källfält för att bygga samma SK som dimmarna
  jdm as (select id, headline        from {{ ref('src_job_details') }}),
  je  as (select id, employer_name   from {{ ref('src_employer') }}),

  -- dimensioner
  dim_occupation            as (select * from {{ ref('dim_occupation') }}),
  dim_job_details           as (select * from {{ ref('dim_job_details') }}),
  dim_employer              as (select * from {{ ref('dim_employer') }}),
  dim_auxilliary_attributes as (select * from {{ ref('dim_auxilliary_attributes') }})

select
  -- valfri egen radnyckel för fact (bra för tester)
  ja.id as job_ad_id,

  -- FK (PK från dimensionerna)
  dim_occupation.occupation_id,
  dim_job_details.job_details_id,
  dim_employer.employer_id,
  dim_auxilliary_attributes.auxilliary_attributes_id,

  -- measures från ads
  ja.vacancies,
  ja.relevance,
  ja.application_deadline

from ja
-- occupation via SK = hash(occupation)
left join dim_occupation
  on dim_occupation.occupation_id = {{ dbt_utils.generate_surrogate_key(['ja.occupation__label']) }}

-- job_details via SK = hash(id, headline)
left join jdm
  on jdm.id = ja.id
left join dim_job_details
  on dim_job_details.job_details_id = {{ dbt_utils.generate_surrogate_key(['jdm.id','jdm.headline']) }}

-- employer via SK = hash(id, employer_name)
left join je
  on je.id = ja.id
left join dim_employer
  on dim_employer.employer_id = {{ dbt_utils.generate_surrogate_key(['je.id','je.employer_name']) }}

-- aux via SK = hash(id)
left join dim_auxilliary_attributes
  on dim_auxilliary_attributes.auxilliary_attributes_id = {{ dbt_utils.generate_surrogate_key(['ja.id']) }}
; #}


--- alex 2 ---


{# with ja as (select * from {{ ref('src_job_ads') }}),     
jd as (select * from {{ ref('src_job_details') }}),     
e  as (select * from {{ ref('src_employer') }}),    
a  as (select * from {{ ref('src_auxilliary_attributes') }}),     
o  as (select * from {{ ref('src_occupation') }}),     

dim_occ as (select * from {{ ref('dim_occupation') }}),     
dim_jd as (select * from {{ ref('dim_job_details') }}),     
dim_e  as (select * from {{ ref('dim_employer') }}),     
dim_a  as (select * from {{ ref('dim_auxilliary_attributes') }}) 

select     
ja.id as job_ad_id, 
dim_occ.occupation_id,     
dim_jd.job_details_id,     
dim_e.employer_id,     
dim_a.auxilliary_attributes_id,     
ja.vacancies,     
ja.relevance,     
ja.application_deadline 
from ja 

left join jd on jd.id = ja.id 
left join e  on e.id = ja.id 
left join a  on a.id = ja.id 
left join o  on o.id = ja.id 
left join dim_occ on dim_occ.occupation = o.occupation 
left join dim_jd on 
dim_jd.employment_type = jd.employment_type    
and dim_jd.duration        = jd.duration    
and dim_jd.salary_type     = jd.salary_type    
and dim_jd.scope_of_work_min = jd.scope_of_work_min    
and dim_jd.scope_of_work_max = jd.scope_of_work_max 
left join dim_e on        
dim_e.employer_name              = e.employer_name    
and dim_e.employer_organization_number = e.employer_organization_number    
and dim_e.workplace_city             = e.workplace_city    
and dim_e.workplace_postcode         = e.workplace_postcode    
and dim_e.workplace_region           = e.workplace_region   
and dim_e.workplace_country          = e.workplace_country
left join dim_a on        dim_a.experience_required = a.experience_required   
and dim_a.driver_license      = a.driver_license   
and dim_a.access_to_own_car   = a.access_to_own_car
  #}

with ja as (select * from {{ ref('src_job_ads') }}),

jd as (select * from {{ ref('src_job_details') }}),

e as (select * from {{ ref('src_employer') }}),

a as (select * from {{ ref('src_auxilliary_attributes') }}),

o as (select * from {{ ref('src_occupation') }})

select
    {{ dbt_utils.generate_surrogate_key(['jd.id', 'jd.headline']) }} as job_details_id,
    {{ dbt_utils.generate_surrogate_key(['jd.id', 'e.employer_name']) }} as employer_id,
    {{ dbt_utils.generate_surrogate_key(['jd.id', 'a.experience_required']) }} as auxilliary_attributes_id,
    {{ dbt_utils.generate_surrogate_key(['jd.id', 'o.occupation']) }} as occupation_id,

    vacancies,
    relevance,
    application_deadline,

    -- Manual testing

    e.employer_name, 
    jd.description,
    o.occupation

    -- borås kommun 5cc72aad4f0096fd5fda1063565eb256
    -- arvidsjaur kommun a95554cd28287de89eec5955242b1e8c
 
from 
    ja
left join
    jd ON ja.id = jd.id
left join
    e on ja.id = e.id
left join
    a on ja.id = a.id
left join
    o on ja.id = o.id

where occupation_id = 'e4ae745a0e55339d00a3a413e52db9b4'
