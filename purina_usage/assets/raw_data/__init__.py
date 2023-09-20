import numpy as np
import pandas as pd
from dagster import asset
from typing import Generator, Any
from dagster import OpExecutionContext, Output

from gql.transport.requests import RequestsHTTPTransport
from purina_usage.utils import random_data
from dagster_insights import DagsterInsightsClient, DagsterInsightsMetric

transport = RequestsHTTPTransport(
    url="http://localhost:3000/test/staging/graphql",
    use_json=True,
    timeout=300,
    headers={"Dagster-Cloud-Api-Token": "user:test:joe"},
)
dagster_insights = DagsterInsightsClient(
    organization_id="test",
    deployment="test",
    cloud_user_token="",
    transport=transport,
)


@asset(compute_kind="random")
def users(context: OpExecutionContext) -> Generator[Output[pd.DataFrame], Any, Any]:
    """A table containing all users data."""
    data = pd.DataFrame(
        {
            "user_id": range(1000),
            "company": np.random.choice(
                ["FoodCo", "ShopMart", "SportTime", "FamilyLtd"], size=1000
            ),
            "is_test_user": np.random.choice([True, False], p=[0.002, 0.998], size=1000),
        }
    )
    yield Output(data)
    dagster_insights.put_context_metrics(
        context,
        metrics=[
            DagsterInsightsMetric(
                metric_name="rows_affected",
                metric_value=len(data),
            )
        ],
    )


@asset(compute_kind="random")
def orders(context: OpExecutionContext) -> Generator[Output[pd.DataFrame], Any, Any]:
    """A table containing all orders that have been placed."""
    data = random_data(
        extra_columns={"order_id": str, "quantity": int, "purchase_price": float, "sku": str},
        n=10000,
    )
    yield Output(data)
    dagster_insights.put_context_metrics(
        context,
        metrics=[
            DagsterInsightsMetric(
                metric_name="rows_affected",
                metric_value=len(data),
            )
        ],
    )
