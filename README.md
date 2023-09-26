# Purina Usage

This project contains testing Dagster pipelines for cost and usage data attribution

## usage

you can refer to the [`snowflake_async_metrics` asset definition](./purina_usage/assets/usage/__init__.py) for an example of an async metric pipeline that will:

1. fetch any asset observations made in the last hour with `query_id` metadata
2. query the snowflake query_history table for the query_ids in step 1
3. write asset metrics pulled from query_history table metric values

## snowflake role

in order to fetch snowflake specific metric data you will need to create a service role that can access your snowflake account's query history table:

```sql
-- Run as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create the role
CREATE ROLE IF NOT EXISTS dagster_snowflake_monitoring_role;
grant role dagster_snowflake_monitoring_role to ROLE ACCOUNTADMIN;

GRANT OPERATE ON WAREHOUSE TINY_WAREHOUSE TO ROLE dagster_snowflake_monitoring_role;
GRANT USAGE ON WAREHOUSE TINY_WAREHOUSE TO ROLE dagster_snowflake_monitoring_role;

-- Allows to query Snowflake metadata
grant imported privileges on database snowflake to role dagster_snowflake_monitoring_role;
 -- Allows to monitor warehouses
grant monitor usage on account to role dagster_snowflake_monitoring_role;

execute immediate $$
declare
  role_name varchar default 'dagster_snowflake_monitoring_role';
  res resultset default (show warehouses);
  cur cursor for res;
begin
  for row_variable in cur do
    execute immediate 'grant monitor on warehouse ' || row_variable."name" || ' to role ' || role_name;
  end for;
  return 'Success!';
end;
$$;

-- Run as query_history_viewer
USE ROLE dagster_snowflake_monitoring_role;

-- Get query history for current user
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
LIMIT 10;


```
