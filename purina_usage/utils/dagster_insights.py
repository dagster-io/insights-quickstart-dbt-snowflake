import os
import requests
import time
import snowflake.connector


from dagster import OpExecutionContext

URL = "https://dagster-insights.dogfood.dagster.cloud/prod/graphql/"
CLOUD_TOKEN = os.getenv("DAGSTER_CLOUD_API_TOKEN")

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


def get_snowflake_usage(query_id, database):
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
        time.sleep(30)
    return rows


def store_dbt_adapter_metrics(
    context: OpExecutionContext,
    manifest,
    run_results,
):
    # store the manifest and run results somewhere
    # for now, we'll just print them
    if (
        manifest["metadata"]["dbt_schema_version"]
        != "https://schemas.getdbt.com/dbt/manifest/v9.json"
    ):
        context.log.warn(
            f"unexpected dbt schema version: {manifest['metadata']['dbt_schema_version']}, required: https://schemas.getdbt.com/dbt/manifest/v9.json"
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
                context.log.info(
                    get_snowflake_usage(
                        result["adapter_response"][adapter_response_key], node["database"]
                    )
                )
            context.log.info(
                f"metric: {node['name']}.{adapter_response_key}: {result['adapter_response'][adapter_response_key]}"
            )
            metric_values.append(
                {
                    "metricValue": result["adapter_response"][adapter_response_key],
                    "metricName": adapter_response_key,
                }
            )
        assetMetricDefinitions.append(
            {
                "assetKey": node["name"],
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
