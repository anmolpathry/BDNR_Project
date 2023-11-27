#!/usr/bin/env python3
from collections import defaultdict
import logging
from tabulate import tabulate
from datetime import datetime

# Set logger
log = logging.getLogger()

CREATE_KEYSPACE = """
    CREATE KEYSPACE IF NOT EXISTS {}
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_FLIGHT_PASSENGERS_TRANSIT_MONTH = """
    CREATE TABLE IF NOT EXISTS flight_passengers_tr_m (
    passenger_id TEXT,
    airline TEXT,
    from_loc TEXT,
    to_loc TEXT,
    day INT,
    month INT,
    year INT,
    age INT,
    gender TEXT,
    reason TEXT,
    stay TEXT,
    transit TEXT,
    connection BOOLEAN,
    wait INT,
    price DECIMAL,
    PRIMARY KEY ((from_loc), month, passenger_id)
    ) WITH CLUSTERING ORDER BY (month DESC)
"""

CREATE_FLIGHT_PASSENGERS_REASON_MONTH = """
    CREATE TABLE IF NOT EXISTS flight_passengers_rm (
    passenger_id TEXT,
    airline TEXT,
    from_loc TEXT,
    to_loc TEXT,
    day INT,
    month INT,
    year INT,
    age INT,
    gender TEXT,
    reason TEXT,
    stay TEXT,
    transit TEXT,
    connection BOOLEAN,
    wait INT,
    price DECIMAL,
    PRIMARY KEY ((from_loc, reason), month, passenger_id)
    ) WITH CLUSTERING ORDER BY (month DESC)
"""

SELECT1 = """
    SELECT from_loc, month, COUNT(month) AS month_count
    FROM flight_passengers_tr_m
    GROUP BY from_loc, month;
"""

SELECT2 = """
    SELECT from_loc, reason, COUNT(reason) AS travelers
    FROM flight_passengers_rm
    WHERE reason = 'On vacation/Pleasure'
    GROUP BY from_loc, reason ALLOW FILTERING;
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_FLIGHT_PASSENGERS_TRANSIT_MONTH)
    session.execute(CREATE_FLIGHT_PASSENGERS_REASON_MONTH)

def get_most_popular_travel_months(session):
    log.info("Retrieving the most popular months for travel")
    stmt = session.prepare(SELECT1)
    rows = session.execute(stmt)

    from_loc_counts = defaultdict(list)

    # Populate the dictionary
    for row in rows:
        from_loc_counts[row.from_loc].append((row.month, row.month_count))

    # Find the top 3 counts for each from_loc
    top_3_counts = {}
    for from_loc, counts in from_loc_counts.items():
        top_3_counts[from_loc] = sorted(counts, key=lambda x: x[1], reverse=True)[:3]

    table_data = []
    for from_loc, top_counts in top_3_counts.items():
        for month, month_count in top_counts:
            month_name = "January" if month == 1 else \
                        "February" if month == 2 else \
                        "March" if month == 3 else \
                        "April" if month == 4 else \
                        "May" if month == 5 else \
                        "June" if month == 6 else \
                        "July" if month == 7 else \
                        "August" if month == 8 else \
                        "September" if month == 9 else \
                        "October" if month == 10 else \
                        "November" if month == 11 else \
                        "December"
            table_data.append([from_loc, month_name, month_count])

    headers = ["Airport", "Month", "Travelers"]
    print(tabulate(table_data, headers=headers, tablefmt="pretty"))

    print("")

def get_most_visited_airports_during_vacation(session):
    log.info("Retrieving The Most Visited Airports During Vacation/Pleasure")
    stmt = session.prepare(SELECT2)
    rows = session.execute(stmt)

    table_data = []
    for row in rows:
        table_data.append([row.from_loc, row.reason, row.travelers])

    headers = ["From Location", "Reason", "Travelers"]
    print(tabulate(table_data, headers=headers, tablefmt="pretty"))
