from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db import models
from app.schemas import patient as patient_schema

router = APIRouter()

@router.get("/", response_model=list[patient_schema.PatientOut])
def list_patients(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return db.query(models.Patient).offset(skip).limit(limit).all()