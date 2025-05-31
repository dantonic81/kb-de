from dagster import Definitions, job, op, ScheduleDefinition, DefaultScheduleStatus
from app.analytics.analytics import analytics_aggregate_biometrics

@op
def aggregate_biometrics_op(context):
    context.log.info("Starting biometrics aggregation job")
    try:
        analytics_aggregate_biometrics()
    except Exception as e:
        context.log.error(f"Aggregation failed: {e}")
        raise
    context.log.info("Aggregation finished successfully")

@job
def aggregate_biometrics_job():
    aggregate_biometrics_op()

hourly_schedule = ScheduleDefinition(
    job=aggregate_biometrics_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING
)

defs = Definitions(
    jobs=[aggregate_biometrics_job],
    schedules=[hourly_schedule]
)
