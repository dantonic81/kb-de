Setup:

bash
docker-compose up -d  # Starts Postgres + API


API Documentation:



Link to FastAPI’s /docs endpoint.

 1. Built-in API Docs (Swagger / ReDoc)


Swagger UI: http://localhost:8000/docs

ReDoc: http://localhost:8000/redoc




Design Decisions:

Why Dagster over cron? ("Scalability and monitoring").

Trend analysis methodology.



run_etl.py - potential improvements: more detailed error reporting (row number, file name), structured logging (json) for better integration with log aggregators e.g. ELK
integrate with monitoring tools (records processed, errors, duration)


get yourself a cup of coffee if you have a slower internet connection :)