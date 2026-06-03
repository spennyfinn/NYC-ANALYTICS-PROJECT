
with stg as (
    select  agency, agency_name
    from {{ref('stg_311__requests')}}
),

deduped as(
    select distinct agency, agency_name
    from stg
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['agency'])}} as agency_key,
        agency,
        agency_name
    from deduped
)

select * from final
