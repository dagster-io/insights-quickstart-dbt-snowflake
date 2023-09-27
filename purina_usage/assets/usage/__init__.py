import datetime
import os


from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

import snowflake.connector
from dagster import HourlyPartitionsDefinition, Output, asset
from dagster_cloud.metrics import DagsterMetric, put_metrics
from dagster_cloud import DagsterCloudAgentInstance

ASSET_OBSERVATIONS_QUERY = """
query AssetQuery(
  $limit: Int
  $cursor: String
  $afterTimestampMillis: String
  $beforeTimestampMillis: String
) {
  assetsOrError(limit: $limit, cursor: $cursor) {
    __typename
    ... on AssetConnection {
      nodes {
        id
        key {
          path
        }
        definition {
          repository {
            origin {
              repositoryName
              repositoryLocationName
            }
          }
          groupName
        }
        assetObservations(
          afterTimestampMillis: $afterTimestampMillis
          beforeTimestampMillis: $beforeTimestampMillis
        ) {
          runId
          stepKey
          timestamp
          metadataEntries {
            label
            __typename
            ... on TextMetadataEntry {
              text
              __typename
            }
          }
        }
      }
    }
  }
}
"""


@asset(
    compute_kind="usage_calculations",
    io_manager_key="model_io_manager",
    partitions_def=HourlyPartitionsDefinition(start_date="2023-09-26-04:00"),
)
def snowflake_async_metrics(context):
    query_id_mappings = {}
    partition_date_str = context.asset_partition_key_for_output()

    end_date = datetime.datetime.strptime(partition_date_str, "%Y-%m-%d-%H:%M").replace(
        tzinfo=datetime.timezone.utc
    )
    start_date = end_date - datetime.timedelta(hours=1, minutes=5)

    context.log.info(f"Querying for {start_date} to {end_date}")

    start_date_timestamp = int(start_date.timestamp() * 1000)
    end_date_timestamp = int(end_date.timestamp() * 1000)
    variables = {
        "afterTimestampMillis": str(start_date_timestamp),
        "beforeTimestampMillis": str(end_date_timestamp),
    }

    if context.instance.organization_name != "test":
        transport = RequestsHTTPTransport(
            url=f"{context.instance.dagit_url}graphql",
            use_json=True,
            timeout=300,
            headers={"Dagster-Cloud-Api-Token": context.instance.dagster_cloud_agent_token},
        )
    else:
        transport = RequestsHTTPTransport(
            url=f"http://127.0.0.1:3000/{context.instance.organization_name}/staging/graphql",
            use_json=True,
            timeout=300,
            headers={"Dagster-Cloud-Api-Token": context.instance.dagster_cloud_agent_token},
        )
    client = Client(transport=transport, fetch_schema_from_transport=True)
    result = client.execute(gql(ASSET_OBSERVATIONS_QUERY), variable_values=variables)
    for node in result["assetsOrError"]["nodes"]:
        for observation in node["assetObservations"]:
            for metadata_entry in observation["metadataEntries"]:
                if metadata_entry["label"] == "query_id":
                    query_id_mappings[metadata_entry["text"]] = {
                        "runId": observation["runId"],
                        "stepKey": observation["stepKey"],
                        "assetKey": "__".join(node["key"]["path"]),
                        "assetGroup": node["definition"]["groupName"],
                        "repositoryName": node["definition"]["repository"]["origin"][
                            "repositoryName"
                        ],
                        "codeLocationName": node["definition"]["repository"]["origin"][
                            "repositoryLocationName"
                        ],
                    }

    context.log.info(f"Found {len(query_id_mappings)} asset observations with query id metadata")
    if len(query_id_mappings) == 0:
        return Output("noop")
    # Establish a connection to the Snowflake DB
    con = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "DEVELOPMENT"),
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
WHERE QUERY_ID IN ({','.join([f"'{k}'" for k in query_id_mappings.keys()])})
AND END_TIME >= CAST(TO_TIMESTAMP('{start_date_timestamp}') AS DATETIME)
AND END_TIME < CAST(TO_TIMESTAMP('{end_date_timestamp}') AS DATETIME)
"""

    # Execute the query
    cur.execute(query)

    # Fetch all the rows
    rows = cur.fetchall()

    context.log.info(f"Found {len(rows)} matching snowflake queries in the last interval")

    metrics_written = 0

    url = os.getenv("DAGSTER_METRICS_DAGIT_URL", f"{context.instance.dagit_url}graphql")
    token = context.instance.dagster_cloud_agent_token
    if not isinstance(context.instance, DagsterCloudAgentInstance):
        context.info.error("metrics is only available when running in Dagster Cloud")
        return Output("error")

    for row in rows:
        if row[0] in query_id_mappings:
            metrics_written += 1
            observation = query_id_mappings[row[0]]

            put_metrics(
                url=url,
                token=token,
                run_id=observation["runId"],
                step_key=observation["stepKey"],
                asset_key=observation["assetKey"],
                asset_group=observation["assetGroup"],
                repository_name=observation["repositoryName"],
                code_location_name=observation["codeLocationName"],
                metrics=[
                    DagsterMetric(metric_name="total_elapsed_time", metric_value=row[1]),
                    DagsterMetric(metric_name="bytes_scanned", metric_value=row[2]),
                    DagsterMetric(metric_name="bytes_written", metric_value=row[3]),
                    DagsterMetric(metric_name="compilation_time", metric_value=row[4]),
                    DagsterMetric(metric_name="execution_time", metric_value=row[5]),
                    DagsterMetric(metric_name="snowflake_credits", metric_value=row[6]),
                ],
            )

    context.log.info(f"Wrote {metrics_written} metric values")
    con.close()
    return Output("complete")
