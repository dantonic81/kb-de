from dagster import Definitions, job, op, ScheduleDefinition, DefaultScheduleStatus
from app.etl.run_etl import run_etl

@op
def etl_op():
    run_etl()

@job
def etl_job():
    etl_op()

# Define the hourly schedule
hourly_etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="0 * * * *",  # Runs at the start of every hour
    execution_timezone="UTC",  # Optional: adjust to your timezone, e.g., "America/New_York"
    default_status=DefaultScheduleStatus.RUNNING
)

# Update Definitions to include the schedule
defs = Definitions(
    jobs=[etl_job],
    schedules=[hourly_etl_schedule]
)