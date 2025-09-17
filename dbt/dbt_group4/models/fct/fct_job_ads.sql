
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

    {# e.employer_name, 
    jd.description,
    o.occupation #}

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


