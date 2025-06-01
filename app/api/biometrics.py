from fastapi import APIRouter, Depends, Response, HTTPException, Query, Path, status
from sqlalchemy import exc
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db import models
from app.schemas import biometric as biometric_schema


router = APIRouter(tags=["biometrics"])


@router.get(
    "/{patient_id}",
    response_model=biometric_schema.BiometricPaginated,
    summary="List biometrics for a patient",
    response_description="Paginated biometric data for the specified patient"
)
def list_biometrics(
    patient_id: int = Path(..., description="ID of the patient"),
    type: str | None = Query(None, description="Optional filter by biometric type"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(10, ge=1, description="Maximum number of records to return"),
    db: Session = Depends(get_db)
):
    """
    Retrieve a paginated list of biometrics for a specific patient.

    - **patient_id**: ID of the patient.
    - **type**: Optional filter by biometric type (e.g., "weight", "blood_pressure").
    - **skip**: Number of records to skip for pagination.
    - **limit**: Maximum number of records to return.
    """
    patient = db.query(models.Patient).get(patient_id)
    if not patient:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Patient {patient_id} not found"
        )

    query = db.query(models.Biometric).filter(
        models.Biometric.patient_id == patient_id
    )

    if type:
        query = query.filter(models.Biometric.biometric_type == type.lower())

    total = query.count()
    results = (
        query.order_by(models.Biometric.timestamp.desc())
             .offset(skip)
             .limit(limit)
             .all()
    )

    return {
        "data": results,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@router.post(
    "/{patient_id}",
    response_model=biometric_schema.BiometricOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create or update a biometric record",
    responses={
        201: {"description": "Biometric record created or updated"},
        404: {"description": "Patient not found"},
        422: {"description": "Validation error"},
        500: {"description": "Internal server error"},
    },
)
def upsert_biometric(
    patient_id: int = Path(..., description="ID of the patient"),
    data: biometric_schema.BiometricUpsert = ...,
    db: Session = Depends(get_db)
):
    """
    Create or update a biometric record for the specified patient.

    - **Blood Pressure**: Must include both systolic and diastolic values.
    - **Other Types**: Must include `value` and `unit`.
    - The record is uniquely identified by `(patient_id, biometric_type, timestamp)`.

    Returns the created or updated biometric entry.
    """
    # Verify patient exists
    if not db.query(models.Patient).get(patient_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Patient {patient_id} not found"
        )

    upsert_data = data.dict()
    upsert_data["patient_id"] = patient_id

    # Validate and normalize fields based on biometric type
    if data.biometric_type == "blood_pressure":
        if not all([data.systolic, data.diastolic]):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Both systolic and diastolic are required for blood pressure"
            )
        upsert_data["value"] = None
    else:
        upsert_data.update({"systolic": None, "diastolic": None})

    # Perform upsert
    stmt = insert(models.Biometric).values(**upsert_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["patient_id", "biometric_type", "timestamp"],
        set_=upsert_data
    )

    try:
        db.execute(stmt)
        db.commit()
        record = db.query(models.Biometric).filter(
            models.Biometric.patient_id == patient_id,
            models.Biometric.biometric_type == data.biometric_type,
            models.Biometric.timestamp == data.timestamp
        ).first()
        return record
    except exc.SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )


@router.delete(
    "/{biometric_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a biometric record",
    responses={
        204: {"description": "Record deleted successfully"},
        404: {"description": "Record not found"},
        500: {"description": "Database error"},
    },
)
def delete_biometric(
    biometric_id: int = Path(..., gt=0, description="ID of the biometric record to delete"),
    db: Session = Depends(get_db),
) -> Response:
    """
    Delete a specific biometric record by ID.

    - Returns 204 on successful deletion
    - Returns 404 if record doesn't exist
    """
    # Verify record exists
    record = db.query(models.Biometric).get(biometric_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Biometric record {biometric_id} not found",
        )

    try:
        db.delete(record)
        db.commit()
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except exc.SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )

@router.get(
    "/{patient_id}/analytics",
    response_model=biometric_schema.AnalyticsPaginated,
    summary="Get processed biometric analytics",
    description="Retrieve hourly aggregated metrics (min/max/avg) for patient biometrics",
    responses={
        200: {"description": "Analytics data returned"},
        404: {"description": "Patient not found"},
    },
)
def get_biometric_analytics(
    patient_id: int = Path(..., gt=0, description="Patient ID"),
    metric: str | None = Query(
        None,
        regex="^(glucose|weight|blood_pressure_systolic|blood_pressure_diastolic)$",
        description="Filter by specific metric type",
    ),
    start_date: datetime | None = Query(
        None,
        description="Start of time range (UTC)",
    ),
    end_date: datetime | None = Query(
        None,
        description="End of time range (UTC)",
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(24, ge=1, le=100),  # Defaults to 1 day of hourly data
    db: Session = Depends(get_db),
):
    """
    Get pre-aggregated biometric analytics (from hourly cronjob).

    - Returns paginated hourly aggregated biometric metrics
    - Supports filtering by metric type and date range
    - Returns 404 if patient not found
    """
    # Verify patient exists
    if not db.query(models.Patient).get(patient_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Patient not found")

    query = db.query(models.PatientBiometricHourlySummary).filter(
        models.PatientBiometricHourlySummary.patient_id == patient_id
    )

    if metric:
        query = query.filter(models.PatientBiometricHourlySummary.biometric_type == metric)

    if start_date:
        query = query.filter(models.PatientBiometricHourlySummary.hour_start >= start_date)

    if end_date:
        query = query.filter(models.PatientBiometricHourlySummary.hour_start <= end_date)

    total = query.count()

    results = query.order_by(models.PatientBiometricHourlySummary.hour_start.desc()) \
                   .offset(skip) \
                   .limit(limit) \
                   .all()

    return {
        "data": results,
        "total": total,
        "skip": skip,
        "limit": limit,
    }