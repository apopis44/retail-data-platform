import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from .assets import (
    dbt_gold,
    dbt_silver,
    dbt_staging,
    semantic_layer_validation,
)

DAGSTER_DBT_CRON = os.getenv(
    "DAGSTER_DBT_CRON",
    "*/10 * * * *",
)

DBT_PIPELINE_ASSETS = [
    dbt_staging,
    dbt_silver,
    dbt_gold,
    semantic_layer_validation,
]

dbt_batch_pipeline_job = define_asset_job(
    name="dbt_batch_pipeline",
    selection=AssetSelection.assets(*DBT_PIPELINE_ASSETS),
    description=(
        "Builds and tests the dbt staging, Silver, and Gold layers, "
        "then validates the semantic layer."
    ),
)

dbt_batch_pipeline_schedule = ScheduleDefinition(
    name="dbt_batch_pipeline_schedule",
    job=dbt_batch_pipeline_job,
    cron_schedule=DAGSTER_DBT_CRON,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs the complete dbt batch transformation pipeline.",
)

defs = Definitions(
    assets=DBT_PIPELINE_ASSETS,
    jobs=[dbt_batch_pipeline_job],
    schedules=[dbt_batch_pipeline_schedule],
)