import os
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    RetryPolicy,
    asset,
)

from .runner import run_cli_command

DBT_PROJECT_DIR = Path(
    os.getenv("DBT_PROJECT_DIR", "/app/dbt")
)

DBT_PROFILES_DIR = Path(
    os.getenv("DBT_PROFILES_DIR", "/app/dbt")
)

DBT_RETRY_POLICY = RetryPolicy(
    max_retries=1,
    delay=60,
)


def build_dbt_layer(
    context: AssetExecutionContext,
    layer_name: str,
) -> MaterializeResult:
    """Build and test one dbt model layer."""

    selection = f"path:models/{layer_name}"

    run_cli_command(
        context,
        [
            "dbt",
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROFILES_DIR),
            "--select",
            selection,
        ],
        working_directory=DBT_PROJECT_DIR,
    )

    return MaterializeResult(
        metadata={
            "dbt_selection": selection,
            "dbt_project_directory": str(DBT_PROJECT_DIR),
        }
    )


@asset(
    group_name="dbt_pipeline",
    compute_kind="dbt",
    retry_policy=DBT_RETRY_POLICY,
    description="Builds and tests the dbt staging models sourced from BigQuery Bronze.",
)
def dbt_staging(
    context: AssetExecutionContext,
) -> MaterializeResult:
    return build_dbt_layer(context, "staging")


@asset(
    deps=[dbt_staging],
    group_name="dbt_pipeline",
    compute_kind="dbt",
    retry_policy=DBT_RETRY_POLICY,
    description="Builds and tests the current-state and history models in BigQuery silver.",
)
def dbt_silver(
    context: AssetExecutionContext,
) -> MaterializeResult:
    return build_dbt_layer(context, "silver")


@asset(
    deps=[dbt_silver],
    group_name="dbt_pipeline",
    compute_kind="dbt",
    retry_policy=DBT_RETRY_POLICY,
    description="Builds and tests the dimensional, fact, and wide-table models in BigQuery Gold.",
)
def dbt_gold(
    context: AssetExecutionContext,
) -> MaterializeResult:
    return build_dbt_layer(context, "gold")


@asset(
    deps=[dbt_gold],
    group_name="dbt_pipeline",
    compute_kind="metricflow",
    retry_policy=DBT_RETRY_POLICY,
    description="Validates the dbt semantic models and metrics against BigQuery.",
)
def semantic_layer_validation(
    context: AssetExecutionContext,
) -> MaterializeResult:
    run_cli_command(
        context,
        [
            "mf",
            "validate-configs",
        ],
        working_directory=DBT_PROJECT_DIR,
    )

    return MaterializeResult(
        metadata={
            "validation_command": "mf validate-configs",
            "dbt_project_directory": str(DBT_PROJECT_DIR),
        }
    )