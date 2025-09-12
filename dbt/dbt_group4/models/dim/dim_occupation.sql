with src_occupational as (select * from {{ ref('src_occupational') }})

select
    {{ dbt_utils.generate_surrogate_key(['occupation']) }} as occupation_id,
    occupation,
    max(occupation_group) as occupation_group,
    max(occupation_field) as occupation_field
from src_occupational
group by occupation