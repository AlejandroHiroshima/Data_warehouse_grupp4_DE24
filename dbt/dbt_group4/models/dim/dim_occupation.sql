WITH src_occupation as (select * from {{ ref('src_occupation') }})

select 

    {{ dbt_utils.generate_surrogate_key(['id', 'occupation']) }} AS occupation_id,
    occupation,
    occupation_group,
    occupation_field
 from src_occupation

 -- Manual testing
 {# where occupation_id = '063e542a74c06d114ecfba0cf028319a' #}

