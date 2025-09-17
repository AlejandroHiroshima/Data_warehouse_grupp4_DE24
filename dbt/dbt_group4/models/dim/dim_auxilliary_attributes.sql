with src_auxilliary_attributes as (select * from {{ ref('src_auxilliary_attributes') }})

select
    {{ dbt_utils.generate_surrogate_key(['id', 'experience_required'])}} as auxilliary_attributes_id,
    experience_required,
    driver_license,
    access_to_own_car
   
from src_auxilliary_attributes