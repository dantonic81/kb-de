from fastapi import APIRouter, Depends, Query, HTTPException, status, Path, Response
from typing import Optional, List
from sqlalchemy import and_
from sqlalchemy import exc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from datetime import datetime
from app.db.session import get_db
from app.db import models
from app.schemas import biometric as biometric_schema

router = APIRouter(tags=["biometrics"])


@router.get("/{patient_id}", response_model=biometric_schema.BiometricPaginated)
def list_biometrics(
    patient_id: int,
    type: str = None,
    skip: int = 0,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    # First verify patient exists
    patient = db.query(models.Patient).get(patient_id)
    if not patient:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Patient {patient_id} not found"
        )

    # Build base query
    query = db.query(models.Biometric).filter(
        models.Biometric.patient_id == patient_id
    )

    # Apply type filter if provided
    if type:
        query = query.filter(models.Biometric.biometric_type == type.lower())

    # Get total count before pagination
    total = query.count()

    # Apply pagination
    results = query.order_by(models.Biometric.timestamp.desc()) \
                 .offset(skip) \
                 .limit(limit) \
                 .all()

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
    summary="Create or update biometric record",
    responses={
        201: {"description": "Biometric record created/updated"},
        404: {"description": "Patient not found"},
        422: {"description": "Validation error"}
    }
)
def upsert_biometric(
        patient_id: int,
        data: biometric_schema.BiometricUpsert,
        db: Session = Depends(get_db)
):
    """
    Create or update a biometric record for a patient.

    - **Blood Pressure**: Provide both systolic and diastolic values
    - **Other Metrics**: Provide value and unit
    - Uses timestamp as part of upsert key
    """
    # Verify patient exists
    if not db.query(models.Patient).get(patient_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Patient {patient_id} not found"
        )

    # Prepare upsert data
    upsert_data = data.dict()
    upsert_data["patient_id"] = patient_id

    # Special handling for blood pressure
    if data.biometric_type == "blood_pressure":
        if not all([data.systolic, data.diastolic]):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Both systolic and diastolic required for blood pressure"
            )
        upsert_data["value"] = None
    else:
        upsert_data.update({"systolic": None, "diastolic": None})

    # Upsert operation
    stmt = insert(models.Biometric).values(**upsert_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["patient_id", "biometric_type", "timestamp"],
        set_=upsert_data
    )

    try:
        result = db.execute(stmt)
        db.commit()
        # Return the upserted record
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
        500: {"description": "Database error"}
    }
)
def delete_biometric(
        biometric_id: int = Path(..., gt=0, description="ID of the biometric record to delete"),
        db: Session = Depends(get_db)
):
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
            detail=f"Biometric record {biometric_id} not found"
        )

    try:
        db.delete(record)
        db.commit()
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except exc.SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )