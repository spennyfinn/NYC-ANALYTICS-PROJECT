with stg as(
    select 
        unique_key,
        agency,
        borough,
        incident_zip,
        incident_address,
        complaint_type,
        descriptor,
        created_at,
        closed_at
    from {{ref('stg_311__requests')}}

),


stg_agency as(
    select *
    from stg as s
    join {{ref('dim_agency')}} as da
    on s.agency = da.agency
),


stg_location as(
    select *
    from stg_agency as sa
    join {{ref('dim_location')}} as dl
        on sa.borough = dl.borough 
        and sa.incident_zip = dl.incident_zip 
        and sa.incident_address = dl.incident_address
),

stg_date as(
    select *
    from stg_location as sl
    join {{ref('dim_date')}} as dd
        on cast(sl.created_at as date) = dd.full_date
),

stg_complaint as(
    select *
    from stg_date as sd
    join {{ref('dim_complaint_type')}} as dct
        on sd.complaint_type = dct.complaint_type 
        and sd.descriptor = dct.descriptor
),

final as (
    select 
        {{dbt_utils.generate_surrogate_key(['unique_key'])}} as service_request_key,
        unique_key,
        date_key,
        agency_key,
        complaint_type_key,
        location_key,
        timestamp_diff(closed_at, created_at, hour) as resolution_time_hours,
        case 
            when closed_at is not null then true
            else false 
            end as is_resolved
    from stg_complaint
)

select * from final