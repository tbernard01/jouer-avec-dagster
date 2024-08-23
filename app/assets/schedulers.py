from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition

from . import add_someone_job

schedule = ScheduleDefinition(
    job=add_someone_job,
    cron_schedule="*/2 * * * 1-5",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs_schedulers = Definitions(schedules=[schedule])
