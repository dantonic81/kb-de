from fastapi import APIRouter, Depends, Query, HTTPException, status, Path
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

