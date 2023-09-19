import numpy as np
import pandas as pd
from dagster import asset
from typing import Generator, Any
from dagster import OpExecutionContext, Output

from purina_usage.utils import random_data
from ...utils import dagster_insights


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
            dagster_insights.Metric(
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
            dagster_insights.Metric(
                metric_name="rows_affected",
                metric_value=len(data),
            )
        ],
    )
