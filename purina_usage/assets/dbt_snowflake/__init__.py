import os
from pathlib import Path

from dagster_dbt import DbtCliResource, dbt_assets

from dagster import OpExecutionContext
from dagster_insights import store_dbt_adapter_metrics

dbt_project_dir = Path(__file__).joinpath("..", "..", "..", "..", "dbt_project").resolve()
dbt_cli_resource = DbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_project_dir.joinpath("config")),
    target="staging",
)

dbt_parse_invocation = dbt_cli_resource.cli(["parse"]).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")


@dbt_assets(manifest=dbt_manifest_path)
def nonblocking_dbt_snowflake_assets(context: OpExecutionContext, dbt: DbtCliResource):
    dbt_cli_invocation = dbt.cli(["build"], context=context)
    yield from dbt_cli_invocation.stream()

    run_results = dbt_cli_invocation.get_artifact("run_results.json")
    manifest = dbt_cli_invocation.get_artifact("manifest.json")
    yield from store_dbt_adapter_metrics(context, manifest, run_results)
