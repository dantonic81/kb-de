from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db import models
from app.schemas import patient as patient_schema

router = APIRouter()


@router.get("/", response_model=list[patient_schema.PatientOut])
def list_patients(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(
        10, ge=1, le=100, description="Maximum number of records to return"
    ),
    db: Session = Depends(get_db),
):
    """
    Retrieve a paginated list of patients.

    - **skip**: Number of records to skip for pagination (default: 0).
    - **limit**: Maximum number of patients to return (default: 10, max: 100).
    - **Returns**: A list of patients in abbreviated format.
    """
    return db.query(models.Patient).offset(skip).limit(limit).all()
