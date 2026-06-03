from dagster import asset
from ingestion.socrata_client import fetch_pages
from ingestion.load_to_bigquery import add_timestamp, load_bigquery_client, load_data_into_bigquery
from ingestion.config import GCP_PROJECT_ID
from google.cloud import bigquery
import os
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject
from datetime import datetime, timedelta, timezone

@asset
def raw_service_request_311():
    client = load_bigquery_client(GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.raw.service_requests_311"

    job_config= bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=False,
        ignore_unknown_values= True,
        schema=[
            bigquery.SchemaField("unique_key", "STRING",mode='REQUIRED', 
                    description="Unique key to identify the 311 service request"),
            bigquery.SchemaField("created_date", 'STRING',
                    description='Date and time the service request was created'),
            bigquery.SchemaField("closed_date", 'STRING',
                    description='Date and time the service request was closed'),
            bigquery.SchemaField('agency', 'STRING',
                    description='Abbreviated name of the NYC agency that handled the request'),
            bigquery.SchemaField('agency_name', "STRING",
                    description='Full name of the NYC agency that handled the request'),
            bigquery.SchemaField('complaint_type', 'STRING',
                    description='Category describing the nature of the service request'),
            bigquery.SchemaField('descriptor', 'STRING',
                    description='More specific detail about the complaint type'),
            bigquery.SchemaField('address_type', 'STRING',
                    description='Type of address associated with the request, such as address or intersection'),
            bigquery.SchemaField('intersection_street_1', 'STRING',
                    description='First street name for intersection-based requests'),
            bigquery.SchemaField('intersection_street_2', 'STRING',
                    description='Second street name for intersection-based requests'),
            bigquery.SchemaField('location_type', "STRING",
                    description='Type of location where the issue was reported'),
            bigquery.SchemaField('incident_zip', 'STRING',
                    description='ZIP code where the incident occurred'),
            bigquery.SchemaField('incident_address', 'STRING',
                    description='Street address where the incident occurred'),
            bigquery.SchemaField('landmark', 'STRING',
                    description='Landmark or building name associated with the incident'),
            bigquery.SchemaField('city', 'STRING',
                    description='City where the incident occurred'),
            bigquery.SchemaField('status', 'STRING',
                    description='Current status of the service request, such as open or closed'),
            bigquery.SchemaField('due_date', 'STRING',
                    description='Date by which the agency is expected to resolve the request'),
            bigquery.SchemaField('resolution_description', 'STRING',
                    description='Text description of how the request was resolved'),
            bigquery.SchemaField('resolution_action_updated_date', 'STRING',
                    description='Date the resolution action was last updated'),
            bigquery.SchemaField('community_board', 'STRING',
                    description='NYC community board number associated with the incident location'),
            bigquery.SchemaField('council_district', 'STRING',
                    description='NYC council district number associated with the incident location'),
            bigquery.SchemaField('borough', 'STRING',
                    description='NYC borough where the incident occurred'),
            bigquery.SchemaField('latitude', 'STRING',
                    description='Latitude coordinate of the incident location'),
            bigquery.SchemaField('longitude', 'STRING',
                    description='Longitude coordinate of the incident location'),
            bigquery.SchemaField('_loaded_at', 'TIMESTAMP',
                    description='Timestamp when this record was loaded into BigQuery'),
        ]
    )
    
    since = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')

    for page in fetch_pages(since, 3000):
        if page is None or len(page)==0:
            raise ValueError('There was no data returned from the API')
        timestamp_data=add_timestamp(page)
        load_data_into_bigquery(table_id, timestamp_data, job_config, client)



dbt_project= DbtProject(
    project_dir= os.path.abspath('dbt_project')
)

@dbt_assets(manifest=dbt_project.manifest_path)
def nyc_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(['build'], context=context).stream()



