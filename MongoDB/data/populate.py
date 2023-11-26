import csv
import os
import requests

BASE_URL = "http://localhost:8000"
DATA = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../flight_passengers.csv'))

def main():
    with open(DATA) as fd:
        flights_csv = csv.DictReader(fd)
        for flight in flights_csv:
            flight['day'] = int(flight['day'])
            flight['month'] = int(flight['month'])
            flight['year'] = int(flight['year'])
            flight['age'] = int(flight['age'])
            flight['connection'] = flight['connection'].lower() == 'true'
            flight['wait'] = int(flight['wait'])
            flight['price'] = float(flight['price'])

            x = requests.post(BASE_URL+"/flight", json=flight)
            
            if not x.ok:
                print(f"Failed to post flight {x} - {flight}")
                print(x.content)
                print(x.json)

if __name__ == "__main__":
    main()