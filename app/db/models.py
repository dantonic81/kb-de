from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint, Date
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db.base import Base

class Patient(Base):
    __tablename__ = "patients"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    dob = Column(Date)
    gender = Column(String)
    address = Column(String)
    email = Column(String(150), unique=True, index=True)
    phone = Column(String)
    sex = Column(String)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    biometrics = relationship("Biometric", back_populates="patient")
    hourly_summaries = relationship("PatientBiometricHourlySummary", back_populates="patient", cascade="all, delete-orphan")

class Biometric(Base):
    __tablename__ = "biometrics"

    id = Column(Integer, primary_key=True, index=True)
    patient_id = Column(Integer, ForeignKey("patients.id"), nullable=False, index=True)
    biometric_type = Column(String(50), nullable=False, index=True)
    value = Column(Float, nullable=True)
    systolic = Column(Integer, nullable=True)
    diastolic = Column(Integer, nullable=True)
    unit = Column(String)
    timestamp = Column(DateTime, nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("patient_id", "biometric_type", "timestamp", name="uq_patient_type_timestamp"),
    )

    patient = relationship("Patient", back_populates="biometrics")


class PatientBiometricHourlySummary(Base):
    __tablename__ = "patient_biometric_hourly_summary"

    id = Column(Integer, primary_key=True, index=True)
    patient_id = Column(Integer, ForeignKey("patients.id"), nullable=False, index=True)
    biometric_type = Column(String, nullable=False, index=True)  # e.g., 'glucose', 'weight', 'blood_pressure_systolic', 'blood_pressure_diastolic'
    hour_start = Column(DateTime, nullable=False, index=True)

    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    avg_value = Column(Float, nullable=True)
    count = Column(Integer, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('patient_id', 'biometric_type', 'hour_start', name='uix_patient_type_hour'),
    )

    patient = relationship("Patient", back_populates="hourly_summaries")


# In your models.py
class BiometricTrend(Base):
    __tablename__ = "biometric_trends"

    id = Column(Integer, primary_key=True)
    patient_id = Column(Integer, ForeignKey('patients.id'), index=True)
    biometric_type = Column(String(50), index=True)
    trend = Column(String(20))  # 'increasing', 'stable', 'decreasing', 'volatile', 'insufficient_data'
    analyzed_at = Column(DateTime)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('patient_id', 'biometric_type', name='uq_patient_biometric_trend'),
    )