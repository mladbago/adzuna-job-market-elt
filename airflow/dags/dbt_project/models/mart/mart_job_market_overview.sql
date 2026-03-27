{{config(
    unique_key='job_id'
)}}

WITH fact_jobs as (
    SELECT * FROM {{ ref('fct_job_postings_cleansed') }}
    {% if is_incremental() %}
        WHERE CREATED_AT > (SELECT MAX(CREATED_AT) FROM {{ this }})
    {% endif %}
), 

dim_company as (
    SELECT * FROM {{ ref('dim_company_cleansed') }}
),

dim_category as (
    SELECT * FROM {{ ref('dim_category_cleansed') }}
),

dim_location as (
    SELECT * FROM {{ ref('dim_location_cleansed') }}
)

SELECT 
    f.JOB_ID,
    f.JOB_TITLE, 
    f.CREATED_AT, 
    c.COMPANY_NAME, 
    l.CITY, 
    l.VOIVODESHIP,
    f.SALARY_MIN, 
    f.SALARY_MAX, 
    CAST((f.SALARY_MIN + f.SALARY_MAX) / 2 AS NUMBER(12,2)) AS SALARY_AVG,
    ca.CATEGORY_LABEL
FROM fact_jobs f 
JOIN dim_company c ON f.COMPANY_KEY = c.COMPANY_KEY
JOIN dim_location l ON f.LOCATION_KEY = l.LOCATION_KEY
JOIN dim_category ca ON f.CATEGORY_KEY = ca.CATEGORY_KEY

