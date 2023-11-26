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
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Show accounts",
        2: "Show positions",
        3: "Show trade history",
        4: "Change username",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

# std no esta pensada para utilizarse, se usara sd 
def print_trade_history_menu():
    thm_options = {
        1: "All",
        2: "Date Range",
        3: "Transaction Type (Buy/Sell)",
        4: "Instrument Symbol",
    }
    for key in thm_options.keys():
        print('    ', key, '--', thm_options[key])


# def set_username():
#     username = input('**** Username to use app: ')
#     log.info(f"Username set to {username}")
#     return username


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

    # username = set_username()

    while(True):
        pass

    # HACER LA OBTENCION DEL ACCOUNT Y PASARSELO A get_user_position COMO PARAMETRO

    # while(True):
    #     print(f"User: {username.capitalize()}")
    #     print_menu()
    #     option = int(input('Enter your choice: '))
    #     while option < 1 or option > 5:
    #         print('Error, option not exist')
    #         option = int(input('Enter your choice: '))
    #     if option == 1:
    #         os.system('clear')
    #         print(f"{username.capitalize()} Accounts")
    #         cassandraModel.get_user_accounts(session, username)
    #     if option == 2:
    #         os.system('clear')
    #         print("Positions")
    #         account = cassandraModel.get_account(session, username)
    #         cassandraModel.get_user_position(session, account)
    #     if option == 3:
    #         os.system('clear')
    #         print("Trade History")
    #         account = cassandraModel.get_account(session, username)
    #         print_trade_history_menu()
    #         tv_option = int(input('Enter your trade view choice: '))
    #         while tv_option < 1 or tv_option > 4:
    #             print('Error, option not exist')
    #             tv_option = int(input('Enter your trade view choice: '))
    #         if tv_option == 1:
    #             os.system('clear')
    #             print("All - Trade History")
    #             cassandraModel.get_all_trade_history(session, account)
    #         if tv_option == 2:
    #             os.system('clear')
    #             print("Date Range - Trade History")
    #             dates = cassandraModel.optional_date(1)
    #             cassandraModel.get_date_range_trades_history(session, account, dates[0], dates[1])
    #         if tv_option == 3:
    #             os.system('clear')
    #             print("Transaction Type - Trade History")
    #             tradeType = cassandraModel.get_type()
    #             dates = cassandraModel.optional_date(0)
    #             cassandraModel.get_type_trades_history(session, account, tradeType, dates)
    #         if tv_option == 4:
    #             os.system('clear')
    #             print("Symbol Trade - Trade History")
    #             symbolTrade = cassandraModel.get_symbol(session, account)
    #             dates = cassandraModel.optional_date(0)
    #             cassandraModel.get_symbol_trades_history(session, account, symbolTrade, dates)
    #     if option == 4:
    #         os.system('clear')
    #         username = set_username()
    #     if option == 5:
    #         exit(0)

if __name__ == '__main__':
    main()
