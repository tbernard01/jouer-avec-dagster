import json
import os

import pandas as pd
from dagster import (
    Config,
    Definitions,
    asset,
    define_asset_job,
    get_dagster_logger,
)

FILE = "data/person.json"
AUGMENTED_FILE = "data/person_augmented.json"


class SomeoneConfig(Config):
    name: str = "Joe"
    age: int = 24


@asset(group_name="create_file")
def initial_file():
    with open(FILE, "w") as file:
        file.write(
            json.dumps(
                [
                    {"name": "Alice", "age": 32},
                    {"name": "Bob", "age": 33},
                    {"name": "Charlie", "age": 34},
                    {"name": "David", "age": 35},
                    {"name": "Eve", "age": 12},
                ]
            )
        )


@asset(deps=[initial_file], group_name="create_file")
def file_with_someone_added(context, config: SomeoneConfig):
    persons = pd.read_json(FILE)
    context.log.info(f"Adding {config.name} with age {config.age}")
    context.log.info(persons.head())
    persons = pd.concat(
        [persons, pd.DataFrame([{"name": config.name, "age": config.age}])],
        ignore_index=True,
    )
    with open(AUGMENTED_FILE, "w") as file:
        persons.to_json(file)


@asset(
    deps=[file_with_someone_added],
    description="Remove the previously created file",
    group_name="rm_file",
)
def file_is_deleted():
    get_dagster_logger().info("Removing file")
    if os.path.exists(AUGMENTED_FILE):
        os.remove(AUGMENTED_FILE)


add_someone_job = define_asset_job(
    name="AddSomeone",
    selection=[initial_file, file_with_someone_added],
    description="Create a list of persons, add someone to it and save it",
)

rm_file_job = define_asset_job(
    name="RmFile",
    selection=[file_is_deleted],
    description="Remove the previously created file",
)

defs_operations = Definitions(
    assets=[initial_file, file_with_someone_added, file_is_deleted],
    jobs=[add_someone_job, rm_file_job],
)
