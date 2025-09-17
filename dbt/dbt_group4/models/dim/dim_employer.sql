{# with src_employer as (select * from {{ ref('src_employer') }})

select  
   
    {{capitalize_first_letter("coalesce(workplace_city, 'Stad ej specifierad')")}} as workplace_city,
    {{capitalize_first_letter("coalesce(workplace_street_address, 'Gatuaddress ej specifierad')")}} as workplace_street_address,
    {{capitalize_first_letter("coalesce(workplace_postcode, 'Postnummer ej specifierad')")}} as workplace_postcode,
    {{ dbt_utils.generate_surrogate_key(['employer_name',  'employer_workplace',
    'employer_organization_number', 'workplace_region','workplace_country', 'workplace_street_address', 'workplace_postcode', 'workplace_city'])}} as employer_id,
    employer_name,
    employer_workplace,
    employer_organization_number,
    workplace_region,
    workplace_country

from src_employer

GROUP by

  employer_name,
  employer_workplace,
  employer_organization_number,
  workplace_region,
  workplace_country,
  workplace_street_address,
  workplace_city,
  workplace_postcode #}


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

