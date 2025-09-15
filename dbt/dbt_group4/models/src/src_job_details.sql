 with stg_job_ads as (select * from {{ source('job_ads', 'stg_ads') }} )

select 
    