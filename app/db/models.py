from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint
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
    hourly_summaries = relationship("PatientBiometricHourlySummary", back_populates="patient", cascade="all, delete-orphan")

class Biometric(Base):
    __tablename__ = "biometrics"

    id = Column(Integer, primary_key=True, index=True)
    patient_id = Column(Integer, ForeignKey("patients.id"))
    biometric_type = Column(String)
    value = Column(Float, nullable=True)
    systolic = Column(Integer, nullable=True)
    diastolic = Column(Integer, nullable=True)
    unit = Column(String)
    timestamp = Column(DateTime)

    patient = relationship("Patient", back_populates="biometrics")

class PatientBiometricHourlySummary(Base):
    __tablename__ = "patient_biometric_hourly_summary"

    id = Column(Integer, primary_key=True, index=True)
    patient_id = Column(Integer, ForeignKey("patients.id"), nullable=False, index=True)
    biometric_type = Column(String, nullable=False, index=True)
    hour_start = Column(DateTime, nullable=False, index=True)  # Timestamp rounded to the hour

    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    avg_value = Column(Float, nullable=True)
    count = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint('patient_id', 'biometric_type', 'hour_start', name='uix_patient_type_hour'),
    )

    # Optional: define relationship if Patient model exists
    patient = relationship("Patient", back_populates="hourly_summaries")