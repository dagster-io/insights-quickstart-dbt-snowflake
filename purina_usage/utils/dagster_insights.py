import os
import requests
import time
import snowflake.connector

from typing import List, NamedTuple

from dagster import OpExecutionContext
import dagster._check as check

URL = "https://dagster-insights.dogfood.dagster.cloud/prod/graphql/"
CLOUD_TOKEN = os.getenv("DAGSTER_CLOUD_API_TOKEN")


class Metric(NamedTuple):
    """This class gives information about a Metric.

    Args:
        metric_name (str): name of the metric
        metric_value (float): value of the metric
    """

    metric_name: str
    metric_value: float


PUT_CLOUD_METRICS_MUTATION = """
mutation CreateOrUpdateExtenalMetrics(
  $metrics: [ExternalMetricInputs]!) {
  createOrUpdateExternalMetrics(metrics: $metrics) {
    __typename
    ... on CreateOrUpdateExternalMetricsSuccess {
      status
      __typename
    }
    ...MetricsFailedFragment
    ...UnauthorizedErrorFragment
    ...PythonErrorFragment
  }
}
fragment PythonErrorFragment on PythonError {
  __typename
  message
  stack
  causes {
    message
    stack
    __typename
  }
}
fragment MetricsFailedFragment on CreateOrUpdateExternalMetricsFailed {
  __typename
  message
}
fragment UnauthorizedErrorFragment on UnauthorizedError {
  __typename
  message
}
"""


def get_snowflake_usage(context, query_id, database):
    con = snowflake.connector.connect(
        user=os.getenv("DAGSTER_INSIGHTS_SNOWFLAKE_USER"),
        password=os.getenv("DAGSTER_INSIGHTS_SNOWFLAKE_PASSWORD"),
        account=os.getenv("DAGSTER_INSIGHTS_SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("DAGSTER_INSIGHTS_SNOWFLAKE_WAREHOUSE"),
        database="SNOWFLAKE",
        schema="ACCOUNT_USAGE",
    )

    # Create a cursor object
    cur = con.cursor()

    # Write a SQL query
    query = f"""
SELECT
    QUERY_ID,
    TOTAL_ELAPSED_TIME,
    BYTES_SCANNED,
    BYTES_WRITTEN,
    COMPILATION_TIME,
    EXECUTION_TIME,
    CREDITS_USED_CLOUD_SERVICES
FROM QUERY_HISTORY
WHERE DATABASE_NAME = '{database}'
AND QUERY_ID = '{query_id}'
"""

    while True:
        # Execute the query
        cur.execute(query)

        # Fetch all the rows
        rows = cur.fetchall()
        if len(rows) > 0:
            break
        context.log.info("waiting for snowflake usage data")
        time.sleep(30)
    return [
        {
            "metricValue": rows[0][6],
            "metricName": "snowflake_credits",
        },
        {
            "metricValue": rows[0][2],
            "metricName": "bytes_processed",
        },
    ]


def store_dbt_adapter_metrics(
    context: OpExecutionContext,
    manifest,
    run_results,
):
    # store the manifest and run results somewhere
    # for now, we'll just print them
    if manifest["metadata"]["dbt_schema_version"] not in [
        "https://schemas.getdbt.com/dbt/manifest/v10.json",
        "https://schemas.getdbt.com/dbt/manifest/v9.json",
    ]:
        context.log.warn(
            f"unexpected dbt schema version: {manifest['metadata']['dbt_schema_version']}, required: https://schemas.getdbt.com/dbt/manifest/v10.json"
        )
        return
    if (
        run_results["metadata"]["dbt_schema_version"]
        != "https://schemas.getdbt.com/dbt/run-results/v4.json"
    ):
        context.log.warn(
            f"unexpected dbt schema version: {manifest['metadata']['dbt_schema_version']}, required: https://schemas.getdbt.com/dbt/run-results/v4.json"
        )
        return
    # store the manifest and run results somewhere
    metric_graphql_input = {}
    assetMetricDefinitions = []
    for result in run_results["results"]:
        node = manifest["nodes"][result["unique_id"]]
        metric_values = []
        for adapter_response_key in result["adapter_response"]:
            if adapter_response_key in ["_message", "code"]:
                continue
            # yield (adapter_response_key, result['adapter_response'][adapter_response_key])
            if adapter_response_key == "query_id" and "database" in node:
                snowflake_metrics = get_snowflake_usage(
                    context, result["adapter_response"][adapter_response_key], node["database"]
                )
                metric_values.append(*snowflake_metrics)
        assetMetricDefinitions.append(
            {
                "assetKey": node["name"],
                "assetGroup": "",
                "metricValues": metric_values,
            }
        )
    metric_graphql_input = {
        "runId": context.run_id,
        "stepKey": context.get_step_execution_context().step.key,
        "codeLocationName": context.dagster_run.external_job_origin.location_name,
        "repositoryName": (
            context.dagster_run.external_job_origin.external_repository_origin.repository_name
        ),
        "assetMetricDefinitions": assetMetricDefinitions,
    }

    context.log.info(metric_graphql_input)
    variables = {"metrics": metric_graphql_input}
    response = requests.post(
        URL,
        json={"query": PUT_CLOUD_METRICS_MUTATION, "variables": variables},
        timeout=300,
        headers={"Dagster-Cloud-Api-Token": CLOUD_TOKEN},
    )
    response.raise_for_status()
    json = response.json()
    return json


def put_context_metrics(
    context: OpExecutionContext,
    metrics: List[Metric],
):
    """Store metrics in the dagster cloud metrics store. This method is useful when you would like to
    store run, asset or asset group materialization metric data to view in the insights UI.

    Currently only supported in Dagster Cloud

    Args:
        metrics (Mapping[str, Any]): metrics to store in the dagster metrics store
    """
    check.list_param(metrics, "metrics", of_type=Metric)
    check.inst_param(context, "context", OpExecutionContext)
    metric_graphql_input = {}

    if context.dagster_run.external_job_origin is None:
        raise Exception("dagster run for this context has not started yet")

    if context.has_assets_def:
        metric_graphql_input = {
            "assetMetricDefinitions": [
                {
                    "runId": context.run_id,
                    "stepKey": context.get_step_execution_context().step.key,
                    "codeLocationName": context.dagster_run.external_job_origin.location_name,
                    "repositoryName": (
                        context.dagster_run.external_job_origin.external_repository_origin.repository_name
                    ),
                    "assetKey": selected_asset_key.to_string(),
                    "assetGroup": context.assets_def.group_names_by_key.get(selected_asset_key, ""),
                    "metricValues": [
                        {
                            "metricValue": metric_def.metric_value,
                            "metricName": metric_def.metric_name,
                        }
                        for metric_def in metrics
                    ],
                }
                for selected_asset_key in context.selected_asset_keys
            ]
        }
    else:
        metric_graphql_input = {
            "jobMetricDefinitions": [
                {
                    "runId": context.run_id,
                    "stepKey": context.get_step_execution_context().step.key,
                    "codeLocationName": context.dagster_run.external_job_origin.location_name,
                    "repositoryName": (
                        context.dagster_run.external_job_origin.external_repository_origin.repository_name
                    ),
                    "metricValues": [
                        {
                            "metricValue": metric_def.metric_value,
                            "metricName": metric_def.metric_name,
                        }
                        for metric_def in metrics
                    ],
                }
            ]
        }

    context.log.info(metric_graphql_input)
    variables = {"metrics": metric_graphql_input}
    response = requests.post(
        URL,
        json={"query": PUT_CLOUD_METRICS_MUTATION, "variables": variables},
        timeout=300,
        headers={"Dagster-Cloud-Api-Token": CLOUD_TOKEN},
    )
    response.raise_for_status()
