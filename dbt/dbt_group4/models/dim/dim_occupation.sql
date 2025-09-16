{# with src_occupational as (select * from {{ ref('src_occupation') }})

select
    {{ dbt_utils.generate_surrogate_key(['occupation']) }} as occupation_id,
    occupation,
    max(occupation_group) as occupation_group,
    max(occupation_field) as occupation_field
from src_occupational

group by 
occupation #}


WITH src_occupation as (select * from {{ ref('src_employer') }})

select 

    {{ dbt_utils.generate_surrogate_key(['id', 'employer_name']) }} AS employer_id,
    employer_name,

    {{ capitalize_first_letter("coalesce(workplace_city, 'stad ej specificerad')") }} AS workplace_city

 from src_employer
