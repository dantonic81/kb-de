# 🧬 Data Platform Project

## Table of Contents

- [Introduction](#introduction)
- [Architecture Overview](#-architecture-overview)
  - [System Architecture Diagram](#system-architecture)
  - [Key Components](#key-components)
- [Project Prerequisites](#prerequisites)
- [Configuration Details](#configuration-details)
- [Project Structure](#project-structure)
- [Project Setup](#project-setup)
- [Dagster Job Orchestration & Workflow Overview ](#dagster-job-orchestration--workflow-overview)
- [ETL Pipeline](#etl-pipeline)
  - [ETL Flow Description](#etl-flow-description)
  - [Data Validation Rules](#data-validation-rules)
  - [Error Handling](#error-handling)
- [Testing](#testing)
- [Project Cleanup](#cleanup)
- [Database Schema](#database-schema)
  - [Overview](#overview)
  - [Entity Descriptions](#entity-descriptions)
    - [Patient](#1-patient)
    - [Biometric](#2-biometric)
    - [Patient Biometric Hourly Summary](#3-patientbiometrichourlysummary)
    - [Biometric Trend](#4-biometrictrend)
  - [Schema Validation](#schema-validation)
- [Analytics Engine (Dagster)](#analytics-engine-dagster)
  - [Workflow Overview](#workflow-overview)
- [API Endpoints](#api-endpoints)
- [API Schema and Validation Overview](#api-schema-and-validation-overview)
- [API Documentation](#api-documentation)
- [Key Design Decisions](#key-design-decisions)
- [Future Improvements](#future-improvements)
- [Limitations](#limitations)


## Introduction

This project demonstrates a backend data integration system designed to ingest, process, and serve biometric health data from healthcare providers and connected devices. It simulates a core component of a patient care platform, focusing on data quality, accessibility, and analytical insight.

**The system is composed of:**

- ETL Pipeline: Extracts structured and unstructured health data, performs validation, normalization, and transformation, and loads it into a database.

- RESTful API: Built with FastAPI, this service exposes endpoints for accessing patient and biometric data, supporting CRUD operations and analytical queries.

- Database Layer: PostgreSQL schema designed for efficient storage and querying of patient records, biometric readings, and analytical metrics.

- Analytics Engine: Hourly cron job processes biometric readings to generate derived metrics (min, max, avg), optimized for large datasets via batch processing.

- Validation & Error Handling: Robust handling of missing values, invalid formats, and outliers ensures the pipeline is resilient to real-world data inconsistencies.

- Job Orchestration & Observability: Managed with Dagster, offering UI-based visibility and control over scheduled and ad-hoc jobs.

- Infrastructure: Containerized using Docker for portability and local orchestration.


**This project can support:**

- Continuous health monitoring for clinics

- Dashboards for biometric trend analytics

- Alerts for abnormal biometric readings


**Technologies used:**

- Python 3.11+

- FastAPI

- SQLAlchemy

- Alembic (for database migrations)

- Pandas

- Docker

- Dagster


This solution is designed with maintainability, scalability, and observability in mind, reflecting engineering best practices within a constrained scope.

## 🧱 Architecture Overview

### System Architecture

![img1][diagram]


### Key Components


| Service          | Description                                                               |
|------------------|---------------------------------------------------------------------------|
| `db`             | PostgreSQL 15 database used as the central data store.                    |
| `migrate`        | One-shot container that applies database schema migrations via Alembic.   |
| `pgadmin`        | Web-based admin interface for PostgreSQL.                                 |
| `api`            | FastAPI application exposing HTTP endpoints and applying DB migrations.   |
| `dagster`        | Dagster webserver for viewing and managing data pipelines.                |
| `dagster-daemon` | Background scheduler and job runner for Dagster.                          |



## Prerequisites

- Python
- Docker
- Docker [Compose](https://docs.docker.com/compose/install/linux/#install-using-the-repository) 
- .env file with required environment variables (provided to you via email)



` Makefile Commands:` 

| Command     | Description                                 |
|-------------|---------------------------------------------|
| `make up`   | Builds and starts all services               |
| `make down` | Stops and removes services and containers    |
| `make logs` | Shows logs for all running containers        |
| `make test` | Runs the test suite                         |


## Configuration Details

PostgreSQL database configuration is specified in the .env file.

    .env file example:

    ENV=
    DATABASE_URL=
    DAGSTER_PG_URL=
    POSTGRES_USER=
    POSTGRES_PASSWORD=
    POSTGRES_DB=
    POSTGRES_HOST=
    POSTGRES_PORT=
    PGADMIN_DEFAULT_EMAIL=
    PGADMIN_DEFAULT_PASSWORD=


## Project Structure

```
📁 project-root/
│
├──📁 alembic/                     # Database migration scripts
│   ├── env.py                   # Migration environment config
│   ├── script.py.mako           # Migration script template
│   └──📁 versions/                # Generated migration scripts
│       ├── 8002a82dd77c_...     # Specific migration
│       └── d5233a8698da_...     # Initial migration
│
├──📁 app/                         # Main application code
│   ├──📁 analytics/               # Analytics processing
│   │   ├── analytics.py         # Core analytics logic
│   │   └── trend_analyzer.py    # Trend analysis
│   │
│   ├──📁 api/                     # API endpoints
│   │   ├── biometrics.py        # Biometrics API
│   │   └── patients.py          # Patients API
│   │
│   ├──📁 core/                    # Core application setup
│   │   └── config.py            # Configuration management
│   │
│   ├──📁 db/                      # Database interactions
│   │   ├── base.py              # Base database models
│   │   ├── models.py            # Data models
│   │   └── session.py           # Database session handling
│   │
│   ├──📁 etl/                     # ETL processes
│   │   └── run_etl.py           # ETL pipeline execution
│   │
│   ├──📁 schemas/                 # Data validation schemas
│   │   ├── biometric.py         # Biometric schemas
│   │   ├── patient.py           # Patient schemas
│   │   └── ...                  # Other schema definitions
│   │
│   └── main.py                  # Application entry point
│
├──📁 dagster_home/                # Dagster data orchestration
│   ├── *.py                     # Dagster pipeline definitions
│   ├── dagster.yaml             # Dagster configuration
│   ├── event_logs/              # Execution logs
│   └── workspace.yaml           # Workspace configuration
│
├──📁 data/                        # Data files and generators
│   ├── biometrics.csv           # Sample biometric data
│   ├── patients.json            # Patient records
│   └── time_series_simulator.py # Data generator
│
├──📁 tests/                       # Test suite
│   ├──📁 analytics/               # Analytics tests
│   ├──📁 api/                     # API tests
│   │   └── v1/                  # API version tests
│   ├──📁 integration/             # Integration tests
│   └──📁 unit/                    # Unit tests
│
├──📁 etl_output/                  # ETL process outputs
│   └── invalid_*.csv/json       # Invalid records
│
├── docker-compose.yml           # Container orchestration
├── Dockerfile                   # Primary container definition
├── Dockerfile.dagster           # Dagster-specific container
├── Makefile                     # Build automation
├── requirements.txt             # Production dependencies
├── requirements-dev.txt         # Development dependencies
├── setup.py                     # Package configuration
└── README.md                    # Project documentation
```


## Project Setup

- Clone the repo 
- Navigate to the project directory:

      cd path-to-your-cloned-repo

- Build and run the Docker-compose project: 

      make up

_Note: On slower connections, the initial Docker build may take several minutes — now’s a good time for a coffee refill._

_Also, I've provided a working pgAdmin service (credentials provided in the .env file), so if you need it, make sure to uncomment it in docker-compose.yml before running the following command_

    

    

This will:

- Start the PostgreSQL database (db) and wait for it to become healthy.

- Launch pgadmin (if uncommented) for optional database inspection via a web UI on http://localhost:8080.

- Run the migrate service to apply Alembic migrations and initialize the database schema.

- Start the FastAPI app (api), which exposes endpoints on http://localhost:8000.

- Bring up the Dagster webserver (dagster), available at http://localhost:3000, for monitoring and running pipelines.

- Start the Dagster daemon (dagster-daemon), responsible for executing scheduled jobs and sensors.

Each service is coordinated using **depends_on** conditions to ensure proper startup order, particularly around the health of the database and the completion of migrations.


## Dagster Job Orchestration & Workflow Overview

Dagster UI is available at [http://localhost:3000](http://localhost:3000)

- Use the `overview` or `jobs` tab to explore pipeline components.
- Trigger ETL or analytics jobs manually from the UI.
- The Dagster daemon automatically executes scheduled jobs every hour.


The patient biometrics analysis system comprises four key batch jobs executed in a logical sequence:

1. Run `simulate_time_series_job` to generate synthetic biometrics data as timestamped CSV files.

2. Run `etl_job` to extract, validate, and load this data into the database (`patients` and `biometrics` tables).

3. Run `aggregate_biometrics_job` to compute hourly summaries and populate the `patient_biometric_hourly_summary` table.

4. Run `trend_analyzer_job` to classify biometric trends and store them in the `biometric_trends` table.


| Job                         | Description                                                                                                                                                                           |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `simulate_time_series_job`  | Simulates hourly biometric data (glucose, weight, blood pressure) per patient and writes it to timestamped CSV files for downstream processing.                                       |
| `etl_job`                   | Extracts, validates, and normalizes patient and biometric data from CSV files. Clean data is upserted into a PostgreSQL database, and invalid records are logged for review.          |
| `aggregate_biometrics_job`  | Aggregates biometric readings by patient and hour using statistical summaries. Results are stored in the patient_biometric_hourly_summary table for efficient querying and analysis.  |
| `trend_analyzer_job`        | Analyzes recent biometric patterns (e.g., increasing, stable, volatile), classifies trends for each patient, and stores them in the biometric_trends table.                           |




## ETL Pipeline

### ETL Flow Description

The ETL pipeline is defined in `app/etl/run_etl.py` and follows this flow:

1. Load source data from `data/` (CSV/JSON).
2. Validate using Pandera schemas.
3. Normalize timestamps, types, units.
4. Store valid records in PostgreSQL.
5. Write invalid records to `etl_output/`.

Run manually with:

    python app/etl/run_etl.py

### Data Validation Rules

Both patient and biometric data go through a validation stage before being processed.

| Field            | Validation Rule                                                         |
|------------------|-------------------------------------------------------------------------|
| Patient Email    | Must be in a valid email format (`.*@.*\..*`)                           |
| Biometric Values | Must fall within accepted medical ranges (e.g., glucose: 70–200 mg/dL)  |
| Timestamps       | Must be in ISO 8601 format                                              |



### Error Handling

- **Invalid data** → Logged to `etl_output/invalid_*.csv` for review.
- **Missing fields** → Either filled with defaults or rejected depending on context.
- **Outliers** → Marked with `is_outlier=True` for downstream handling.



## Testing

It's recommended to use a separate **virtual environment** when working with this project. While setting up a virtual environment is outside the scope of this document, once it's activated, install the development dependencies:


    pip install -r requirements-dev.txt


Then, to run the full test suite:
   
    make test

or manually:

    pytest tests/


## Cleanup

Once you no longer need these containers running and you no longer need the data either, simply turn the whole thing off by running the following:

    make down

_💡 Note: This will stop and remove the containers, but volumes (e.g., database data) may persist unless explicitly configured otherwise in docker-compose.yml._





## Database Schema

### Overview

The system is centered around patients and their biometric data, including both raw measurements and summarized or analyzed trends. The database schema has been designed with normalization in mind, ensuring data consistency, referential integrity, and analytical flexibility.

### Entity Descriptions

#### 1. Patient

This table stores personal information about each patient.

- Primary Key: id

- Unique Constraint: email

- Relationships:

  - One-to-many with Biometric

  - One-to-many with PatientBiometricHourlySummary

  - One-to-many with BiometricTrend

#### 2. Biometric

Stores raw biometric measurements.

- Primary Key: id

- Foreign Key: patient_id → Patient.id

- Composite Unique Constraint: (patient_id, biometric_type, timestamp)

- Fields allow storing both scalar (e.g., weight, glucose) and compound (e.g., blood pressure systolic/diastolic) values.

#### 3. PatientBiometricHourlySummary

Captures hourly aggregates of biometric data.

- Primary Key: id

- Foreign Key: patient_id → Patient.id

- Composite Unique Constraint: (patient_id, biometric_type, hour_start)

- Aggregated values include min_value, max_value, avg_value, and count.


#### 4. BiometricTrend

Stores analyzed trends derived from biometric data.

- Primary Key: id

- Foreign Key: patient_id → Patient.id

- Composite Unique Constraint: (patient_id, biometric_type)

- trend is a categorical field indicating the nature of biometric changes (e.g., increasing, decreasing, stable).


### Schema Validation

Two Pandera schemas ensure data integrity for incoming data pipelines:

**BiometricSchema:** Ensures valid biometric input with regex pattern checks and biometric type validation.

**PatientSchema:** Enforces presence and type of core patient information fields.


## Analytics Engine (Dagster)

The analytics pipeline is orchestrated by Dagster and runs hourly via the dagster-daemon.


### Workflow Overview:


- Input: Pulls biometric readings from the past hour.

- Processing:

  - Computes min, max, and average values per patient and biometric type.

  - Analyzes trends using moving averages:

    - Classified as improving, stable, or declining.

- Output: Writes results to patient_biometric_hourly_summary table in the database.




## API Endpoints


| Endpoint                             | Method | Description                                            |
|--------------------------------------|--------|--------------------------------------------------------|
| `/patients`                          | GET    | List patients (paginated)                              |
| `/patients/{id}/biometrics`          | GET    | Get biometric history (filter by `?type=glucose`)      |
| `/biometrics`                        | POST   | Upsert a biometric reading                             |
| `/biometrics/{id}`                   | DELETE | Delete a biometric record by its ID                    |
| `/biometrics/{patient_id}/analytics` | GET    | Retrieve hourly biometric stats for a specific patient |


**Example Request:**

    curl "http://localhost:8000/biometrics/1?type=glucose&limit=2" | jq    

**Example Response:**

    {
      "data": [
        {
          "biometric_type": "glucose",
          "timestamp": "2025-06-02T23:00:00",
          "unit": "mg/dL",
          "value": 117.0,
          "systolic": null,
          "diastolic": null,
          "id": 1051,
          "patient_id": 1
        },
        {
          "biometric_type": "glucose",
          "timestamp": "2025-06-02T22:00:00",
          "unit": "mg/dL",
          "value": 123.0,
          "systolic": null,
          "diastolic": null,
          "id": 1036,
          "patient_id": 1
        }
      ],
      "total": 71,
      "skip": 0,
      "limit": 2
    }


## API Schema and Validation Overview

This project uses Pydantic models to define and validate API data structures.

To keep our API data consistent and reliable, we use a system that defines clear rules for the kind of information we accept and return.

We specify what fields are required, what types of data are allowed, and when certain details are necessary. For example, some measurements need extra details only if they belong to a specific category, like blood pressure.

The system automatically checks incoming data against these rules, so incorrect or incomplete information is caught early.

It also formats the data properly when sending it back, making sure things like dates are easy to understand.

This approach helps prevent errors, ensures data quality, and makes the whole API smoother to use and maintain.


## API Documentation


API Documentation is handled by Built-in API Docs (Swagger / ReDoc)


Swagger UI: http://localhost:8000/docs

ReDoc: http://localhost:8000/redoc


## ✅ Functional and Non-Functional Requirements


### Functional Requirements


- Ingest biometric and patient data via ETL

- Expose CRUD API endpoints for patient and biometric data

- Aggregate biometrics hourly per patient

- Analyze trends in patient biometrics (e.g., increasing/decreasing)

- Provide job orchestration and scheduling via Dagster

### Non-Functional Requirements

- Scalability: Batch-processing of large datasets and aggregation jobs

- Resilience: ETL handles missing values, malformed data, and schema violations gracefully

- Observability: Dagster UI provides operational visibility; logs invalid data records

- Maintainability: Modular codebase, use of SQLAlchemy, Alembic, Docker

- Portability: Docker-based deployment with .env for environment separation

- Testability: Structured test suite for unit and integration testing

- Extensibility: Schema and job definitions are adaptable for new sources or metrics


## Key Design Decisions

- **Idempotent ETL** — Retry-safe pipeline to prevent duplicate records.

- **Batch Processing** — Uses pandas for efficient in-memory transformations.

- **Trend Analysis** — Relies on simple moving averages to detect stability or changes.

- **Containerization** — Docker-based architecture for modular and scalable deployment.


## Future Improvements

- **Stream Processing** — Introduce Kafka/Flink for real-time data ingestion and processing.

- **GraphQL API** — Add flexible, client-driven data querying alongside REST.

- **Anomaly Detection** — Leverage ML models to flag unusual biometric patterns more accurately.

- Support for multi-tenant architecture

- Integration with visualization tools (e.g. Metabase or Superset)


## Limitations

- No authentication/authorization on API endpoints (intended for local demo).
- Not tested for production load or concurrent writes.
- Not horizontally scalable at current stage
- Assumes synthetic data
- Limited real-world validation
- Data persistence is local unless configured otherwise.
- Scheduler assumes system time is synced across services.




## CI/CD or Deployment

This project is designed for local development. Deployment to cloud infrastructure (e.g., AWS, GCP) is out of scope but could be integrated with Terraform and CI pipelines.



[diagram]: Screenshots/Diagram.PNG "System Architecture Diagram"