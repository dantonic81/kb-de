# app/schemas/base.py
from pydantic import BaseModel
from typing import List, Generic, TypeVar

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    data: List[T]
    total: int
    skip: int
    limit: int
