import json

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetOut,
    Definitions,
    Out,
    asset,
    multi_asset,
    op,
)
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type

OrdersDataFrame = create_dagster_pandas_dataframe_type(
    name="OrdersDataFrame",
    columns=[
        PandasColumn.string_column("user"),
        PandasColumn.string_column("product", is_required=False),
    ],
)


@multi_asset(
    outs={
        "users": AssetOut(),
        "orders": AssetOut(),
    },
    group_name="raw_data",
)
def datalake():
    users = [
        {"name": "Alice", "age": 32},
        {"name": "Bob", "age": 33},
        {"name": "Charlie", "age": 34},
        {"name": "David", "age": 35},
        {"name": "Eve", "age": 12},
    ]
    orders = [
        {"user": "Alice", "product": "Apple"},
        {"user": "Bob", "product": "Banana"},
        {"user": "Bob", "product": "Apple"},
        {"user": "Charlie", "product": "Carrot"},
        {"user": "Charlie", "product": "Apple"},
        {"user": "Tim", "product": None},
    ]

    return users, orders


@op()
def dump_users(context: AssetExecutionContext, users: list[dict]):
    context.log.info(f"Received {users}")
    with open("data/users.json", "w") as file:
        json.dump(
            users,
            file,
        )


@op(
    out=Out(OrdersDataFrame),
)
def make_orders() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"user": "Alice", "product": "Apple"},
            {"user": "Bob", "product": "Banana"},
            {"user": "Bob", "product": "Apple"},
            {"user": "Charlie", "product": "Carrot"},
            {"user": "Charlie", "product": "Apple"},
            {"user": "Tim", "product": None},
        ]
    )


# @asset(
#     group_name="raw_data",
#     freshness_policy=FreshnessPolicy(maximum_lag_minutes=1),
# )
# def dump_orders(context: AssetExecutionContext) -> pd.DataFrame:
#     df = make_orders()
#     context.log_event(
#         AssetObservation(asset_key="orders", metadata={"n_rows": len(df)})
#     )
#     return df


# @asset(group_name="cleaned_data")
# def orders_cleaned(dump_orders: pd.DataFrame):
#     df = pd.DataFrame(dump_orders)
#     df = df.dropna(subset=["product"])

#     df.to_json("data/orders_cleaned.json", orient="records")


@asset(group_name="cleaned_data")
def orders_cleaned(orders: list[dict]):
    df = pd.DataFrame(orders)
    df = df.dropna(subset=["product"])

    df.to_json("data/orders_cleaned.json", orient="records")


@asset(group_name="cleaned_data", deps=["users"])
def users_cleaned():
    with open("data/users.json", "r") as file:
        users = json.load(file)

    df = pd.DataFrame(users)
    df = df[df["age"] >= 18]

    df.to_json("data/users_cleaned.json", orient="records")


defs_data = Definitions(assets=[datalake, orders_cleaned, users_cleaned])
