from dagster import Definitions, job, op, ScheduleDefinition, DefaultScheduleStatus
from app.analytics.trend_analyzer import main


@op
def trend_analyzer_op(context):
    context.log.info("Starting biometrics aggregation job")
    try:
        main()
    except Exception as e:
        context.log.error(f"Aggregation failed: {e}")
        raise
    context.log.info("Aggregation finished successfully")


@job
def trend_analyzer_job():
    trend_analyzer_op()


hourly_schedule = ScheduleDefinition(
    job=trend_analyzer_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(jobs=[trend_analyzer_job], schedules=[hourly_schedule])
