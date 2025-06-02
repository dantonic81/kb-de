from pydantic import BaseModel
from datetime import date

class PatientBase(BaseModel):
    name: str
    dob: date
    gender: str
    address: str
    email: str
    phone: str
    sex: str

class PatientCreate(PatientBase):
    pass

class PatientOut(PatientBase):
    id: int

    class Config:
        orm_mode = True