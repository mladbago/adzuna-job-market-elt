SELECT 
    JOB_ID, 
    SALARY_MIN, 
    SALARY_MAX 
FROM {{ ref('fct_job_postings_cleansed') }}
WHERE SALARY_MIN > SALARY_MAX