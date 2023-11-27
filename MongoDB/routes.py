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
def list_flights(request: Request):
    flights = list(request.app.database["flights"].find({}))

    return flights

@router.get("/price", response_description="Get all flights", response_model=List)
def list_flights(request: Request, price:float=0):
    query = {"price": {"$lte": price}}
    projection = {"from_loc": 1, "to_loc": 1, "price": 1, "airline": 1, "_id": 0}
    flights = list(request.app.database["flights"].find(query, projection))

    return flights


@router.get("/airports", response_description="Get all airports for food/beverages", response_model=List)
def list_airports(request: Request):
    pipeline = [
            {"$match": {"wait": {"$gt": 0}}},
            {
                "$group": {
                    "_id": "$to_loc",
                    "totalWait": {"$sum": "$wait"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "airport": "$_id",
                    "totalWait": 1
                }
            },
            {"$sort": {"totalWait": -1, "airport": 1}}
        ]

    flights = list(request.app.database["flights"].aggregate(pipeline))

    return flights