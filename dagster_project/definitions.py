from dagster import Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource
from dagster_project.assets import raw_service_request_311, nyc_dbt_assets
import os



pipeline_job = define_asset_job(name='nyc_pipeline_job')

daily_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 6 * * *"
)

defs = Definitions(
    assets = [raw_service_request_311, nyc_dbt_assets],
    schedules=[daily_schedule],
    resources = {
        "dbt": DbtCliResource(project_dir=os.path.abspath("dbt_project"))
    }
)