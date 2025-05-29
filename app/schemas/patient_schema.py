import pandera as pa
from pandera import Column, DataFrameSchema, Check

PatientSchema = DataFrameSchema({
    "name": Column(pa.String, nullable=False),
    "dob": Column(pa.String, nullable=False, checks=Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
    "gender": Column(pa.String, nullable=True),
    "address": Column(pa.String, nullable=True),
    "email": Column(pa.String, nullable=False, checks=Check.str_contains("@")),
    "phone": Column(pa.String, nullable=True),
    "sex": Column(pa.String, nullable=True),
})
