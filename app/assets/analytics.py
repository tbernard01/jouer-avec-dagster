import pandas as pd
from dagster import Definitions, asset


@asset(
    group_name="cleaned_data",
    description="merge data",
    deps=["orders_cleaned", "users_cleaned"],
)
def merged_data() -> pd.DataFrame:
    orders = pd.read_json("data/orders_cleaned.json")
    users = pd.read_json("data/users_cleaned.json")
    return pd.merge(orders, users, left_on="user", right_on="name")


@asset(group_name="analytics")
def users_products(context, merged_data: pd.DataFrame) -> pd.Series:
    products = merged_data.groupby("name")["product"].apply(list)
    context.log.info(products)
    return products


@asset(group_name="analytics")
def product_counts(context, merged_data: pd.DataFrame) -> pd.Series:
    counts = merged_data["product"].value_counts()
    context.log.info(counts)
    return counts


defs_analyse = Definitions(assets=[product_counts, users_products, merged_data])
