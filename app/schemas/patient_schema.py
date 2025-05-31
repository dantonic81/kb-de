import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check

PatientSchema = DataFrameSchema({
    "name": Column(str, nullable=False),
    "dob": Column(str, nullable=False),
    "email": Column(str, nullable=False),
    "gender": Column(str, nullable=True),
    "address": Column(str, nullable=True),
    "phone": Column(str, nullable=True),
    "sex": Column(str, nullable=True)
})