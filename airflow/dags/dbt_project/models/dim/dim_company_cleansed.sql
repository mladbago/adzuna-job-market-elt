WITH UNIQUE_COMPANIES AS (
    SELECT DISTINCT 
        COMPANY_NAME
    FROM {{ ref('silver_adzuna_cleansed') }} 
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['COMPANY_NAME'])}} AS COMPANY_KEY,
    CAST(COMPANY_NAME AS VARCHAR(128)) AS COMPANY_NAME
FROM UNIQUE_COMPANIES