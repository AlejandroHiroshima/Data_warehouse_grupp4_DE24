WITH src_employer as (select * from {{ ref('src_employer') }})

select 

    {{ dbt_utils.generate_surrogate_key(['id', 'employer_name']) }} AS employer_id,
  employer_name,
  employer_workplace,
  employer_organization_number,
  {{ capitalize_first_letter("coalesce(workplace_street_address, 'Address ej specificerad')") }} AS workplace_street_address,
  {{ capitalize_first_letter("coalesce(workplace_postcode, 'Postnummer ej specificerad')") }} AS workplace_postcode,
  {{ capitalize_first_letter("coalesce(workplace_city, 'Stad ej specificerad')") }} AS workplace_city,
  workplace_region,
  workplace_country,
  
 from 
    src_employer

