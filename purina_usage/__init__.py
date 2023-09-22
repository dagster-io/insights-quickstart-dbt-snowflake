from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    fs_io_manager,
    load_assets_from_package_module,
)
from dagster_snowflake_pandas import snowflake_pandas_io_manager

from purina_usage.assets import raw_data, usage, dbt_snowflake

# import os
# from pathlib import Path

# from dagster_dbt import DbtCliResource

# dbt_project_dir = Path(__file__).joinpath("..", "..", "dbt_project").resolve()
# dbt_cli_resource = DbtCliResource(
#     project_dir=os.fspath(dbt_project_dir),
#     profiles_dir=os.fspath(dbt_project_dir.joinpath("config")),
#     target="staging",
# )

# dbt_parse_invocation = dbt_cli_resource.cli(["parse"]).wait()
# dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")

dbt_snowflake_assets = load_assets_from_package_module(
    dbt_snowflake,
    group_name="snowflake_dbt",
    # all of these assets live in the snowflake database, under the schema raw_data
    key_prefix=["snowflake", "dbt"],
)

raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="raw_data",
    # all of these assets live in the snowflake database, under the schema raw_data
    key_prefix=["raw_data"],
)

usage_assets = load_assets_from_package_module(
    usage,
    group_name="snowflake_insights",
    # all of these assets live in the snowflake database, under the schema usage_data
    key_prefix=["snowflake", "insights_data"],
)

# define jobs as selections over the larger graph
raw_job = define_asset_job("raw_job", selection=["raw_data/users", "raw_data/orders"])

from gql.transport.requests import RequestsHTTPTransport

transport = RequestsHTTPTransport(
    url="http://localhost:3000/test/staging/graphql",
    use_json=True,
    timeout=300,
    headers={"Dagster-Cloud-Api-Token": "user:test:joe"},
)


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
    "dbt": dbt_snowflake.dbt_cli_resource,
}


defs = Definitions(
    assets=[*dbt_snowflake_assets, *raw_data_assets, *usage_assets],
    resources=resources,
    schedules=[
        ScheduleDefinition(job=raw_job, cron_schedule="@daily"),
    ],
)
