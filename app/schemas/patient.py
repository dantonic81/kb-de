from pydantic import BaseModel

class PatientBase(BaseModel):
    name: str
    dob: str
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