with 
    fct fct_job_ads as (select * from {{ ref('fct_job_ads')}}),
    dim_employer as (select * from {{ ref('dim_employer')}}),
    dim_occupation as (select * from {{ ref('dim_occupation')}})
    dim_job_details as (select * from {{ ref('dim_job_details') }})
    dim_auxilliary_attributes as (select * from {{ ref('dim_auxilliary_attributes') }})

select
    fct.occupation_id,
    fct.job_details_id,
    fct.employer_id,
    fct.auxilliary_attributes_id,
    de.



    f.vacancies,
    f.application_deadline,
from fct_job_ads f
left join dim_occupation o on f.occupation_id = o.occupation_id