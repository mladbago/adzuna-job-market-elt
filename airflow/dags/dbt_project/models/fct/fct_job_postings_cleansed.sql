{{config(
    unique_key='job_posting_key'
)}}

WITH CLEANED_DATA AS (
    SELECT * FROM {{ ref('silver_adzuna_cleansed') }}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['JOB_ID']) }} AS JOB_POSTING_KEY,
    {{ dbt_utils.generate_surrogate_key(['CATEGORY_LABEL', 'CATEGORY_TAG']) }} AS CATEGORY_KEY,
    {{ dbt_utils.generate_surrogate_key(['COMPANY_NAME']) }} AS COMPANY_KEY,
    {{ dbt_utils.generate_surrogate_key(['CITY', 'VOIVODESHIP']) }} AS LOCATION_KEY,
    CAST(TO_CHAR(CREATED_AT, 'YYYYMMDD') AS NUMBER(8,0)) AS DATE_KEY,
    JOB_ID, 
    JOB_TITLE, 
    SALARY_MIN, 
    SALARY_MAX, 
    JOB_DESCRIPTION,
    JOB_URL,
    LOADED_AT, 
    CREATED_AT
FROM CLEANED_DATA

{% if is_incremental() %}
    WHERE LOADED_AT > (SELECT MAX(LOADED_AT) FROM {{ this }})
{% endif %}

