import datetime
import json
import pydgraph
from tabulate import tabulate

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
        gender: string .
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
        airline: uid @reverse .
        passenger: uid .
        to_loc_airport: [uid] @count .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def get_flight_routes(client):
    query = """query popular_flight_routes(){
         popular_routes(func: has(day)) {
            from_loc: from_loc {
                airport_name
            }
            to_loc: to_loc {
                airport_name
            }
            flight_count: count(uid)
        }
    }
    """

    res = client.txn(read_only=True).query(query)
    routes = json.loads(res.json)

    return routes 

def show_flight_routes(client):
    routes = get_flight_routes(client)
    #print(routes)
    routes = routes["popular_routes"]
    duplicates = {}

    # Iterar sobre las rutas y verificar duplicados
    for route in routes:
        if "from_loc" in route and "to_loc" in route:
            from_airport = route["from_loc"]["airport_name"]
            to_airport = route["to_loc"]["airport_name"]
            route_key = f"{from_airport} to {to_airport}"

            if route_key in duplicates:
                duplicates[route_key].append(route)
            else:
                duplicates[route_key] = [route]

    # Filtrar rutas duplicadas
    duplicates = {key: value for key, value in duplicates.items() if len(value) > 1}

    sorted_duplicates = sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True)

    # Imprimir las primeras cinco rutas duplicadas
    top_5_duplicates = sorted_duplicates[:5]
    table = []

    for key, value in top_5_duplicates:
       table.append([f"Route {key}", len(value)])

    print(tabulate(table, headers=["Route", "Count"], tablefmt="pretty"))


def show_flight_airlines(client):
    query = """query flight_airlines(){
          flights(func: has(airline_name)) {
            airline_name
            flight_count: count(~airline)
        }
    }
    """

    res = client.txn(read_only=True).query(query)
    airlines = json.loads(res.json)

    print(f"Number of Airlines: {len(airlines['flights'])}")

    table = []
    for airline in airlines['flights']:
        table.append([airline['airline_name'], airline['flight_count']])

    # Print the table.
    print(tabulate(table, headers=["Airline Name", "Flight Count"], tablefmt="pretty"))




def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))
