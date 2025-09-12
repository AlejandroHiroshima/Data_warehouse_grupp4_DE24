    -- this is am extract of the model

    -- CTE part of clean code
    -- this with create temporary data for the final selcet statement

    with stg_job_ads as (select * from {{ source('job_ads', 'stg_ads') }} )

    -- Select statement is major part of the model
    SELECT
        occupation__label,
        number_of_vacancies as vacancies,
        relevance,
        application_deadline
    FROM stg_job_ads
    order by application_deadline