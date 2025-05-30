from dagster import Definitions, job, op
from app.etl.run_etl import main

@op
def etl_op():
    main()

@job
def etl_job():
    etl_op()

defs = Definitions(jobs=[etl_job])
