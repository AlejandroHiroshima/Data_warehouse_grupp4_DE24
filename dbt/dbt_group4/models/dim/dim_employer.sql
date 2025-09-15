with src_employer as (select * from {{ ref('src_employer') }})

select  
    {{ dbt_utils.generate_surrogate_key(['id', 'employer_name'])}} as employer_id, employer_name,

    {{capitalize_first_letter("coalesce(workplace_city, 'Stad ej specifierad')")}} as workplace_city,
    {{capitalize_first_letter("coalesce(workplace_street_address, 'Gatuaddress ej specifierad')")}} as workplace_street_address,
    {{capitalize_first_letter("coalesce(workplace_postcode, 'Postnummer ej specifierad')")}} as workplace_postcode,
    employer_workplace,
    employer_organization_number,
    workplace_region,
    workplace_country

from src_employer