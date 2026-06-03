{% snapshot dim_status_scd2 %}

{{
    config(
        target_schema = 'analytics',
        unique_key = 'unique_key',
        strategy='check',
        check_cols=['status']
    )
}}

select 
    unique_key,
    status,
    created_at,
    _loaded_at
from {{ref('stg_311__requests')}}

{% endsnapshot %}