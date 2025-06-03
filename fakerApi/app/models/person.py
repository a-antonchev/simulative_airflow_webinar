from datetime import datetime
import uuid

from pydantic import BaseModel
from typing import Optional


class PersonResponse(BaseModel):
    id: uuid.UUID
    name: str
    age: int
    address: str
    email: str
    phone_number: str
    registration_date: datetime
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None
