import os
from pathlib import Path

from dagster_dbt import DbtCliResource, dbt_assets

from dagster import OpExecutionContext
from dagster_insights import DagsterInsightsResource, SnowflakeConnectionDetails

snowflake_connection_details = SnowflakeConnectionDetails(
    user=os.getenv("SNOWFLAKE_USER", ""),
    password=os.getenv("SNOWFLAKE_PASSWORD", ""),
    account="na94824.us-east-1",
    warehouse="DEVELOPMENT",
)

dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "dbt_project").resolve()
dbt_cli_resource = DbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_project_dir.joinpath("config")),
    target="staging",
)

dbt_parse_invocation = dbt_cli_resource.cli(["parse"]).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")


@dbt_assets(manifest=dbt_manifest_path)
def blocking_dbt_snowflake_assets(
    context: OpExecutionContext, dbt: DbtCliResource, dagster_insights: DagsterInsightsResource
):
    dbt_cli_invocation = dbt.cli(["build"], context=context)
    yield from dbt_cli_invocation.stream()

    run_results = dbt_cli_invocation.get_artifact("run_results.json")
    manifest = dbt_cli_invocation.get_artifact("manifest.json")
    dagster_insights.store_dbt_adapter_metrics(
        context, manifest, run_results, snowflake_connection_details
    )
