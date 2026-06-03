-- Select everything from our raw database
with source as (
    select * from {{source('raw_311', 'service_requests_311')}}
), 

deduped as (
    select *
    from source
    qualify row_number() over(
        partition by unique_key
        order by _loaded_at desc
    ) = 1

),


renamed as (
    select 
        unique_key,
        agency,
        agency_name,
        complaint_type,
        descriptor,
        borough,
        incident_zip,
        incident_address,
        city,
        community_board,
        council_district,
        status,
        cast(latitude as float64) as latitude,
        cast(longitude as float64) as longitude,
        cast(created_date as timestamp) as created_at,
        cast(closed_date as timestamp) as closed_at ,
        cast(due_date as timestamp) as due_at,
        _loaded_at
    from deduped
)

select * from renamed