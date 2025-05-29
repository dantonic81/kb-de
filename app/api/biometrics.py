from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db import models
from app.schemas import biometric as biometric_schema

router = APIRouter()

@router.post("/{patient_id}")
def upsert_biometric(patient_id: int, data: biometric_schema.BiometricIn, db: Session = Depends(get_db)):
    biometric = models.Biometric(**data.dict(), patient_id=patient_id)
    db.add(biometric)
    db.commit()
    db.refresh(biometric)
    return biometric

@router.get("/{patient_id}")
def list_biometrics(patient_id: int, type: str = None, skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    query = db.query(models.Biometric).filter(models.Biometric.patient_id == patient_id)
    if type:
        query = query.filter(models.Biometric.biometric_type == type)
    return query.offset(skip).limit(limit).all()