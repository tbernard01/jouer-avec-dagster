from dagster import (
    AssetIn,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
    op,
)


#########################
# Définir les opérations #
#########################
@op(description="Add one to any int")
def add_one(input_num: int) -> int:
    return input_num + 1


@op(description="multiply by two")
def multiply_by_two(input_num: int) -> int:
    return input_num * 2


#########################
#   Définir les assets   #
#########################


@asset(group_name="mathematics")
def one() -> int:
    return 1


@asset(group_name="mathematics")
def seven() -> int:
    return 7


@asset(ins={"one": AssetIn("one")}, group_name="mathematics", key="four")
def four(one: int) -> int:
    return multiply_by_two(add_one(one))


@asset(
    ins={"four": AssetIn("four"), "seven": AssetIn("seven")},
    group_name="mathematics",
    description="Salut la guilde ! Cet asset crée le nombre 11 grâce à tous les assets précédents",
)
def eleven(context, four: int, seven: int) -> int:
    context.log.info("Creating eleven ! Hello La guilde DE")
    return four + seven


job_make_eleven = define_asset_job(
    name="Maths",
    selection=AssetSelection.groups("mathematics"),
    description="Le but de ce job est de créer le nombre 11. Il utilise les assets one, four et seven pour le faire.",
)

schedule = ScheduleDefinition(
    job=job_make_eleven,
    cron_schedule="5 * * * 1-5",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[one, four, seven, eleven], jobs=[job_make_eleven], schedules=[schedule]
)
