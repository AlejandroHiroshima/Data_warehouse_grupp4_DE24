

    with stg_job_ads as (select * from {{ source('job_ads', 'stg_ads') }} )

    
    SELECT
        id,
        headline,
        occupation__label,
        number_of_vacancies as vacancies,
        relevance,
        application_deadline
    FROM stg_job_ads
    order by application_deadline
    