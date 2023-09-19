import os
from pathlib import Path

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    fs_io_manager,
    load_assets_from_package_module,
)
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_snowflake_pandas import snowflake_pandas_io_manager

from purina_usage.assets import raw_data, usage

from dagster import OpExecutionContext
from .utils import dagster_insights

dbt_project_dir = Path(__file__).joinpath("..", "..", "dbt_project").resolve()
dbt = DbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_project_dir.joinpath("config")),
    target="staging",
)

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_parse_invocation = dbt.cli(["parse"]).wait()
    dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")


# class CustomDagsterDbtTranslator(DagsterDbtTranslator):
#     @classmethod
#     def get_group_name(cls, _unused):
#         return "test_group"


@dbt_assets(manifest=dbt_manifest_path)
def dbt_snowflake_assets(context: OpExecutionContext, dbt: DbtCliResource):
    dbt_cli_invocation = dbt.cli(["build"], context=context)
    yield from dbt_cli_invocation.stream()

    run_results = dbt_cli_invocation.get_artifact("run_results.json")
    manifest = dbt_cli_invocation.get_artifact("manifest.json")
    dagster_insights.store_dbt_adapter_metrics(context, manifest, run_results)


def partition_key_to_dbt_vars(partition_key):
    return {"run_date": partition_key}


# all assets live in the default dbt_schema
# dbt_assets = load_assets_from_dbt_project(
#     DBT_PROJECT_DIR,
#     DBT_PROFILES_DIR,
#     # prefix the output assets based on the database they live in plus the name of the schema
#     key_prefix=["snowflake", "dbt_schema"],
#     # prefix the source assets based on just the database
#     # (dagster populates the source schema information automatically)
#     source_key_prefix=["snowflake"],
#     # partitions_def=DailyPartitionsDefinition(start_date="2023-07-01"),
#     # partition_key_to_vars_fn=partition_key_to_dbt_vars,
# )

raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="raw_data",
    # all of these assets live in the snowflake database, under the schema raw_data
    key_prefix=["snowflake", "raw_data"],
)

usage_assets = load_assets_from_package_module(
    usage,
    group_name="snowflake_usage",
    # all of these assets live in the snowflake database, under the schema usage_data
    key_prefix=["snowflake", "usage_data"],
)

# define jobs as selections over the larger graph
raw_job = define_asset_job(
    "raw_job", selection=["snowflake/raw_data/users", "snowflake/raw_data/orders"]
)
# dbt_job = define_asset_job(
#     "dbt_job",
#     selection=[
#         "snowflake/dbt_schema/company_perf",
#         "snowflake/dbt_schema/company_stats",
#         "snowflake/dbt_schema/daily_order_summary",
#         "snowflake/dbt_schema/order_stats",
#         "snowflake/dbt_schema/orders_augmented",
#         "snowflake/dbt_schema/orders_cleaned",
#         "snowflake/dbt_schema/sku_stats",
#         "snowflake/dbt_schema/top_users",
#         "snowflake/dbt_schema/users_cleaned",
#     ],
# )

resources = {
    # this io_manager allows us to load dbt models as pandas dataframes
    "io_manager": snowflake_pandas_io_manager.configured(
        {
            "database": "USAGE_TEST",
            "account": {"env": "SNOWFLAKE_ACCOUNT"},
            "user": {"env": "SNOWFLAKE_USER"},
            "password": {"env": "SNOWFLAKE_PASSWORD"},
            "warehouse": "DEVELOPMENT",
        }
    ),
    # this io_manager is responsible for storing/loading our pickled machine learning model
    "model_io_manager": fs_io_manager,
    # this resource is used to execute dbt cli commands
    "dbt": dbt,
}


defs = Definitions(
    assets=[dbt_snowflake_assets, *raw_data_assets, *usage_assets],
    resources=resources,
    schedules=[
        ScheduleDefinition(job=raw_job, cron_schedule="@daily"),
        # ScheduleDefinition(job=dbt_job, cron_schedule="*/5 * * * *"),
    ],
)
