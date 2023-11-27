import uuid
from pydantic import BaseModel, Field

class FlightInfo(BaseModel):
    id: str = Field(default_factory=uuid.uuid4, alias="_id")
    airline: str = Field(...)
    from_loc: str = Field(...)
    to_loc: str = Field(...)
    day: int = Field(...)
    month: int = Field(...)
    year: int = Field(...)
    age: int = Field(...)
    gender: str = Field(...)
    reason: str = Field(...)
    stay: str = Field(...)
    transit: str = Field(...)
    connection: bool = Field(...)
    wait: int = Field(...)
    price: float = Field(...)

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "_id": "066de609-b04a-4b30-b46c-32537c7f1f6e",
                "airline": "Example Airlines",
                "from_loc": "CityA",
                "to_loc": "CityB",
                "day": 1,
                "month": 1,
                "year": 2023,
                "age": 25,
                "gender": "Male",
                "reason": "Business",
                "stay": "Hotel",
                "transit": "Airport",
                "connection": True,
                "wait": 2,
                "price": 500.0
            }
        }
