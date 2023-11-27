import datetime
import json
import pydgraph

# Crear esquema
def set_schema(client):
    schema = """
        type Airport {
            airport_name
            to_loc_airport
        }

        type Airline {
            airline_name
        }

        type Person {
            age
            gender
            reason
            stay
        }

        type Flight {
            day
            month
            year
            connection
            wait
            price
            from_loc
            to_loc
            airline
            passenger
        }

        airport_name: string @index(hash) .
        airline_name: string @index(hash) .
        age: int .
        gender: string @index(hash) .
        reason: string .
        stay: string .
        day: int .
        month: int .
        year: int .
        connection: bool .
        wait: int .
        price: int .
        from_loc: uid .
        to_loc: uid .
        airline: uid .
        passenger: uid .
        to_loc_airport: [uid] @count .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))
