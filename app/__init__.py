from dagster import Definitions, SensorDefinition

from .assets import analytics, initial_data, operations, schedulers, sensors
from .assets import assets as ass

definitions = Definitions.merge(
    ass.defs,
    initial_data.defs_data,
    analytics.defs_analyse,
    schedulers.defs_schedulers,
    operations.defs_operations,
    sensors.defs_sensors,
)
