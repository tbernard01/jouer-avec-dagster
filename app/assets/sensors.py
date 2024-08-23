import os

from dagster import (
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    get_dagster_logger,
    sensor,
)

from . import rm_file_job as rm

AUGMENTED_FILE = "data/person_augmented.json"


@sensor(
    job=rm,
    minimum_interval_seconds=60 * 5,
    default_status=DefaultSensorStatus.STOPPED,
)
def is_augmented_file_created():
    get_dagster_logger().warning(
        f"Checking if {AUGMENTED_FILE} exists : {os.path.exists(AUGMENTED_FILE)}"
    )
    if os.path.exists(AUGMENTED_FILE):
        yield RunRequest(
            run_key="remove_augmented_data",
        )


defs_sensors = Definitions(jobs=[rm], sensors=[is_augmented_file_created])
