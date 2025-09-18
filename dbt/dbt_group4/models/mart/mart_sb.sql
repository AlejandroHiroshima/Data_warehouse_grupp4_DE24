with
    fct_job_ads as (select * from {{ ref('fct_job_ads')}}),
    dim_employer as (select * from {{ ref('dim_employer')}}),
    dim_occupation as (select * from {{ ref('dim_occupation')}}),
    dim_job_details as (select * from {{ ref('dim_job_details') }}),
    dim_auxilliary_attributes as (select * from {{ ref('dim_auxilliary_attributes') }})

select
    jd.headline,
    f.vacancies,
    jd.salary_type,
    f.relevance,
    e.employer_name, 
    e.workplace_city, 
    e.workplace_region,
    jd.description,
    jd.description_html_formatted,
    jd.duration,
    jd.scope_of_work_min,
    jd.scope_of_work_max,
    f.application_deadline,
    o.occupation_group,
    o.occupation

from fct_job_ads f

left join dim_occupation o on f.occupation_id = o.occupation_id
left join dim_employer e on f.employer_id = e.employer_id
left join dim_job_details jd on f.job_details_id = jd.job_details_id
left join dim_auxilliary_attributes a on f.auxilliary_attributes_id = a.auxilliary_attributes_id
where o.occupation_field = 'Säkerhet och bevakning'