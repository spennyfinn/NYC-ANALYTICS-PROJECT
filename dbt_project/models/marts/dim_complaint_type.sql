with stg as (
    select complaint_type, descriptor
    from {{ref('stg_311__requests')}}
),

deduped as (
    select distinct complaint_type, descriptor
    from stg
),

final as(
    select 
        {{dbt_utils.generate_surrogate_key(['complaint_type', 'descriptor'])}} as complaint_type_key,
        complaint_type,
        descriptor
    from deduped
)

select * from final
