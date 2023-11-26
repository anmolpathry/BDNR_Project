from typing import List
from fastapi import APIRouter, Body, Request, Response, HTTPException, status
from fastapi.encoders import jsonable_encoder
from model import FlightInfo

router = APIRouter()

@router.post("/", response_description="Post a new flight", status_code=status.HTTP_201_CREATED, response_model=FlightInfo)
def create_flight(request: Request, flight: FlightInfo = Body(...)):
    flight = jsonable_encoder(flight)
    new_flight = request.app.database["flights"].insert_one(flight)
    created_flight = request.app.database["flights"].find_one(
        {"_id": new_flight.inserted_id}
    )

    return created_flight

@router.get("/", response_description="Get all flights", response_model=List[FlightInfo])
def list_books(request: Request):
    flights = list(request.app.database["flights"].find({}))

    return flights