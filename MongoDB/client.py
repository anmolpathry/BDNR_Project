import argparse
import logging
import os
import requests

import json

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('books.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to API connection
FLIGHTS_API_URL = os.getenv("FLIGHTS_API_URL", "http://localhost:8000")

def print_flight(flight):
    for k in flight.keys():
        print(f"{k}: {flight[k]}")
    print("="*50)

def list_flights():
    suffix = "/flights"
    endpoint = FLIGHTS_API_URL + suffix
    # params = {
    #     "rating": rating,
    #     "num_pages": num_pages,
    #     "text_reviews_count": text_reviews_count,
    #     "title": title,
    #     "limit":limit,
    #     "skip":skip
    # }
    #response = requests.get(endpoint, params=params)
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

    list_of_actions = ["search", "get"]
    parser.add_argument("action", choices=list_of_actions,
            help="Action to be user for the flights catalog")
    #parser.add_argument("-t", "--title",
     #       help="Search parameter to look for books with title that includes the param", default=None)


    args = parser.parse_args()

    if args.action == "search":
        list_flights()


if __name__ == "__main__":
    main()