{% snapshot adzuna_jobs_snapshot %}
{{
    config(
        target_schema = 'snapshots', 
        unique_key = 'job_id', 
        strategy = 'timestamp', 
        updated_at = 'loaded_at', 
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('silver_adzuna_cleansed') }}

{% endsnapshot %}