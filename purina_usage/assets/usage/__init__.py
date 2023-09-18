import datetime
import os

import requests
import snowflake.connector
from dagster import DailyPartitionsDefinition, Output, asset

url = "http://localhost:3000/graphql"

SNOWFLAKE_ASSET_MATERIALIZATIONS = """
query LastPartitionAssetMaterializations( $limit: Int, $cursor: String) {
  assetsOrError(limit: $limit, cursor: $cursor) {
    __typename
    ... on AssetConnection {
      nodes {
        id
        key {
          path
        }
        definition {
          assetKey {
            path
          }
          groupName
          repository {
            name
            location {
              name
            }
          }
        }
        assetMaterializations {
          runId
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
    partitions_def=DailyPartitionsDefinition(start_date="2023-09-16"),
)
def snowflake_usage_model(context):
    query_id_mappings = {}
    partition_date_str = context.asset_partition_key_for_output()
    start_date = datetime.datetime.strptime(partition_date_str, "%Y-%m-%d").replace(
        tzinfo=datetime.timezone.utc
    )
    end_date = start_date + datetime.timedelta(days=1)

    start_date_timestamp = int(start_date.timestamp() * 1000)
    end_date_timestamp = int(end_date.timestamp() * 1000)
    variables = {"afterTimestampMillis": start_date_timestamp}
    response = requests.post(
        url, json={"query": SNOWFLAKE_ASSET_MATERIALIZATIONS, "variables": variables}, timeout=300
    )
    response.raise_for_status()
    json = response.json()
    for node in json["data"]["assetsOrError"]["nodes"]:
        for materialization in node["assetMaterializations"]:
            for metadata_entry in materialization["metadataEntries"]:
                if metadata_entry["label"] == "Query ID":
                    query_id_mappings[metadata_entry["text"]] = {
                        "runId": materialization["runId"],
                        "asset": "/".join(node["key"]["path"]),
                    }
    # Establish a connection to the Snowflake DB
    con = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="DEVELOPMENT",
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
WHERE DATABASE_NAME = 'USAGE_TEST'
AND END_TIME >= CAST(TO_TIMESTAMP('{start_date_timestamp}') AS DATE)
AND END_TIME < CAST(TO_TIMESTAMP('{end_date_timestamp}') AS DATE)
"""

    # Execute the query
    cur.execute(query)

    # Fetch all the rows
    rows = cur.fetchall()
    metadata = {}

    for row in rows:
        if row[0] in query_id_mappings:
            metadata[f"{query_id_mappings[row[0]]['asset']}:TOTAL_ELAPSED_TIME"] = row[1]
            metadata[f"{query_id_mappings[row[0]]['asset']}:BYTES_SCANNED"] = row[2]
            metadata[f"{query_id_mappings[row[0]]['asset']}:BYTES_WRITTEN"] = row[3]
            metadata[f"{query_id_mappings[row[0]]['asset']}:COMPILATION_TIME"] = row[4]
            metadata[f"{query_id_mappings[row[0]]['asset']}:EXECUTION_TIME"] = row[5]
            metadata[f"{query_id_mappings[row[0]]['asset']}:CREDITS_USED_CLOUD_SERVICES"] = row[6]
            context.log.info(
                {
                    "TOTAL_ELAPSED_TIME": row[1],
                    "BYTES_SCANNED": row[2],
                    "BYTES_WRITTEN": row[3],
                    "COMPILATION_TIME": row[4],
                    "EXECUTION_TIME": row[5],
                    "CREDITS_USED_CLOUD_SERVICES": row[6],
                }
            )

    # Don't forget to close the connection
    con.close()
    return Output(None, metadata=metadata)
