{{config(
    unique_key='job_id'
)}}

WITH raw_source AS (
    SELECT * FROM {{ source('adzuna_raw', 'jobs_raw') }}
),
FLATTENED AS (
    SELECT 
        F.VALUE:id::STRING AS JOB_ID, 
        F.VALUE:title::STRING AS JOB_TITLE, 
        F.VALUE:description::STRING AS JOB_DESCRIPTION,
        F.VALUE:company:display_name::STRING AS COMPANY_NAME, 
        F.VALUE:location:area[1]::STRING AS VOIVODESHIP, 
        F.VALUE:location:area[2]::STRING AS CITY, 
        F.VALUE:location:display_name::STRING AS LOCATION_FULL, 
        F.VALUE:salary_min::FLOAT AS SALARY_MIN, 
        F.VALUE:salary_max::FLOAT AS SALARY_MAX,
        F.VALUE:redirect_url::STRING AS JOB_URL,
        F.VALUE:contract_type::STRING AS CONTRACT_TYPE,
        F.VALUE:category:label::STRING as CATEGORY_LABEL, 
        F.VALUE:category:tag::STRING AS CATEGORY_TAG, 
        F.VALUE:created::TIMESTAMP_NTZ AS CREATED_AT, 
        INGESTED_AT AS LOADED_AT
    FROM raw_source, 
    LATERAL FLATTEN(input => raw_content:results) AS F
)
SELECT * FROM FLATTENED

{% if is_incremental() %}
    WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
{% endif %}