from fastapi import APIRouter
from app.api import patients, biometrics

router = APIRouter()

router.include_router(patients.router, prefix="/patients", tags=["Patients"])
router.include_router(biometrics.router, prefix="/biometrics", tags=["Biometrics"])
