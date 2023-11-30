#!/usr/bin/env python3
import logging
import os
import random

from cassandra.cluster import Cluster

import cassandraModel

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'flights')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Optimal Months for Advertising Campaigns",
        2: "The Most Visited Airports During Vacation/Pleasure",
        0: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)

def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    cassandraModel.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    cassandraModel.create_schema(session)

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        while option < 0 or option > 2:
            print('Error, option not exist')
            option = int(input('Enter your choice: '))
        if option == 1:
            os.system('clear')
            cassandraModel.get_most_popular_travel_months(session)
        if option == 2:
            os.system('clear')
            cassandraModel.get_most_visited_airports_during_vacation(session)
        if option == 0:
            os.system('clear')
            exit(0)

if __name__ == '__main__':
    main()
