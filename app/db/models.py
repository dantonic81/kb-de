from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from app.db.base import Base

class Patient(Base):
    __tablename__ = "patients"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    dob = Column(String)
    gender = Column(String)
    address = Column(String)
    email = Column(String, unique=True)
    phone = Column(String)
    sex = Column(String)

    biometrics = relationship("Biometric", back_populates="patient")

class Biometric(Base):
    __tablename__ = "biometrics"

    id = Column(Integer, primary_key=True, index=True)
    patient_id = Column(Integer, ForeignKey("patients.id"))
    biometric_type = Column(String)
    value = Column(Float)
    unit = Column(String)
    timestamp = Column(DateTime)

    patient = relationship("Patient", back_populates="biometrics")