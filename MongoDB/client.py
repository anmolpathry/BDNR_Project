import argparse
import logging
import os
import requests

import json

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('flights.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to API connection
FLIGHTS_API_URL = os.getenv("FLIGHTS_API_URL", "http://localhost:8000")

def print_flight(flight):
    for k in flight.keys():
        print(f"{k}: {flight[k]}")
    print("="*50)

def list_flights():
    suffix = "/flight"
    endpoint = FLIGHTS_API_URL + suffix
    response = requests.get(endpoint)
    if response.ok:
        json_resp = response.json()
        for flight in json_resp:
            print_flight(flight)
    else:
        print(f"Error: {response}")

def list_airports():
    #filtrar por mayor wait, ya que aquí se abriran los servicios de alimentos y bebidas porque la gente se queda más tiempo ahí
    suffix = "/flight/airports"
    endpoint = FLIGHTS_API_URL + suffix
    response = requests.get(endpoint)
    if response.ok:
        json_resp = response.json()
        for flight in json_resp:
            print_flight(flight)
    else:
        print(f"Error: {response}")

def main():
    log.info(f"Welcome to flights catalog. App requests to: {FLIGHTS_API_URL}")

    parser = argparse.ArgumentParser()

    list_of_actions = ["search"]
    parser.add_argument("action", choices=list_of_actions,
            help="Action to be user for the flights catalog")
    parser.add_argument("-f", "--flights", nargs='?',
           help="Search parameter to look for flights data", const=True)
    parser.add_argument("-a", "--airports", nargs='?',
           help="Search parameter to look for airports to open food/drinks services", const=True)


    args = parser.parse_args()

    if args.action == "search" and args.flights:
        list_flights()
    if args.action == "search" and args.airports:
        list_airports()


if __name__ == "__main__":
    main()