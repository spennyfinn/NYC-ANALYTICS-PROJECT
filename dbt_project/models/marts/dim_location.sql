with stg as(
    select 
        borough,
        incident_zip,
        incident_address,
        city,
        latitude,
        longitude
    from {{ref('stg_311__requests')}}
),

deduped as(
    select distinct borough, incident_zip, incident_address, city, longitude, latitude
    from stg
    qualify row_number() over (
        partition by borough, incident_zip, incident_address
        order by latitude desc nulls last
    ) = 1
),


final as(
    select 
        {{dbt_utils.generate_surrogate_key(['borough', 'incident_zip', 'incident_address'])}} as location_key,
        borough,
        incident_zip,
        incident_address,
        city,
        latitude,
        longitude
    from deduped
)

select * from final

