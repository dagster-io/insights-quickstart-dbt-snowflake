from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    fs_io_manager,
    load_assets_from_package_module,
)
from dagster._utils import file_relative_path
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_snowflake_pandas import snowflake_pandas_io_manager

from purina_usage.assets import raw_data, usage

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_project/config")


def partition_key_to_dbt_vars(partition_key):
    return {"run_date": partition_key}


# all assets live in the default dbt_schema
dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    DBT_PROFILES_DIR,
    # prefix the output assets based on the database they live in plus the name of the schema
    key_prefix=["snowflake", "dbt_schema"],
    # prefix the source assets based on just the database
    # (dagster populates the source schema information automatically)
    source_key_prefix=["snowflake"],
    # partitions_def=DailyPartitionsDefinition(start_date="2023-07-01"),
    # partition_key_to_vars_fn=partition_key_to_dbt_vars,
)

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
dbt_job = define_asset_job(
    "dbt_job",
    selection=[
        "snowflake/dbt_schema/company_perf",
        "snowflake/dbt_schema/company_stats",
        "snowflake/dbt_schema/daily_order_summary",
        "snowflake/dbt_schema/order_stats",
        "snowflake/dbt_schema/orders_augmented",
        "snowflake/dbt_schema/orders_cleaned",
        "snowflake/dbt_schema/sku_stats",
        "snowflake/dbt_schema/top_users",
        "snowflake/dbt_schema/users_cleaned",
    ],
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
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_DIR,
            "profiles_dir": DBT_PROFILES_DIR,
            "target": "staging",
        }
    ),
}


defs = Definitions(
    assets=[*dbt_assets, *raw_data_assets, *usage_assets],
    resources=resources,
    schedules=[
        ScheduleDefinition(job=raw_job, cron_schedule="@daily"),
        ScheduleDefinition(job=dbt_job, cron_schedule="*/5 * * * *"),
    ],
)
