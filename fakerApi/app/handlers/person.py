from datetime import datetime
import uuid

from fastapi import APIRouter
from faker import Faker
from app.models.person import PersonResponse

router = APIRouter(prefix="/person", tags=["person"])

fake = Faker(locale="ru_RU")


@router.get("/", response_model=PersonResponse)
async def get_person():
    person = PersonResponse(
        id=uuid.uuid4(),
        name=fake.name(),
        age=fake.random_int(min=18, max=99),
        address=fake.address(),
        email=fake.email(),
        phone_number=fake.phone_number(),
        registration_date=fake.date_time_between(start_date="-1y", end_date="now"),
        created_at=fake.date_time_between(start_date="-1y", end_date="now"),
        updated_at=datetime.now(),
        deleted_at=None,
    )
    return person
