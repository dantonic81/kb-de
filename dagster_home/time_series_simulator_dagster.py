from dagster import Definitions, job, op, ScheduleDefinition, DefaultScheduleStatus
from data.time_series_simulator import simulate_and_write

@op
def simulate_time_series_op():
    simulate_and_write()

@job
def simulate_time_series_job():
    simulate_time_series_op()

# Define the hourly schedule
hourly_etl_schedule = ScheduleDefinition(
    job=simulate_time_series_job,
    cron_schedule="0 * * * *",  # Runs at the start of every hour
    execution_timezone="UTC",  # Optional: adjust to your timezone, e.g., "America/New_York"
    default_status=DefaultScheduleStatus.RUNNING
)

# Update Definitions to include the schedule
defs = Definitions(
    jobs=[simulate_time_series_job],
    schedules=[hourly_etl_schedule]
)