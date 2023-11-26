#!/usr/bin/env python3
import logging
from tabulate import tabulate
from datetime import datetime

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username TEXT,
        account_number TEXT,
        cash_balance DECIMAL,
        name TEXT STATIC,
        PRIMARY KEY ((username),account_number)
    )
"""

CREATE_POSSITIONS_BY_ACCOUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS positions_by_account (
        account TEXT,
        symbol TEXT,
        quantity DECIMAL,
        PRIMARY KEY ((account),symbol)
    )
"""

CREATE_TRADES_BY_ACCOUNT_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_d (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), trade_id)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""

# CREADAS POR LA TAREA
CREATE_TRADES_BY_ACCOUNT_TRADE_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_td (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), type, trade_id)
    ) WITH CLUSTERING ORDER BY (type ASC, trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_SYMBOL_TRADE_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_std (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), symbol, type, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, type ASC, trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_SYMBOL_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_sd (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), symbol, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, trade_id DESC)
"""
# AQUI TERMINAN LAS TABLAS DE LA TAREA

SELECT_USER_ACCOUNTS = """
    SELECT username, account_number, name, cash_balance
    FROM accounts_by_user
    WHERE username = ?
"""

# SELECTS DE TAREA

SELECT_USER_POSITIONS = """
    SELECT account, symbol, quantity
    FROM positions_by_account
    WHERE account = ?
"""

SELECT_ALL_TRADES_FROM_HISTORY = """
    SELECT account, toDate(trade_id) AS Date, type, symbol, shares, price, amount
    FROM trades_by_a_d
    WHERE account = ?
"""

SELECT_DATE_RANGE_TRADES_FROM_HISTORY = """
    SELECT account, toDate(trade_id) AS Date, type, symbol, shares, price, amount
    FROM trades_by_a_d
    WHERE account = ?
    AND trade_id >= minTimeuuid(?) AND trade_id <= maxTimeuuid(?)
"""

SELECT_TYPE_TRADES_FROM_HISTORY = """
    SELECT account, toDate(trade_id) AS Date, type, symbol, shares, price, amount
    FROM trades_by_a_td
    WHERE account = ?
    AND type = ?
"""

SELECT_TYPE_TRADES_FROM_HISTORY_BY_DATE = """
    SELECT account, toDate(trade_id) AS Date, type, symbol, shares, price, amount
    FROM trades_by_a_td
    WHERE account = ?
    AND type = ?
    AND trade_id >= minTimeuuid(?) AND trade_id <= maxTimeuuid(?)
"""

SELECT_SYMBOL_TRADES_FROM_HISTORY = """
    SELECT account, toDate(trade_id) AS Date, type, symbol, shares, price, amount
    FROM trades_by_a_sd
    WHERE account = ?
    AND symbol = ?
"""

SELECT_SYMBOL_TRADES_FROM_HISTORY_BY_DATE = """
    SELECT account, toDate(trade_id) AS Date, type, symbol, shares, price, amount
    FROM trades_by_a_sd
    WHERE account = ?
    AND symbol = ?
    AND trade_id >= minTimeuuid(?) AND trade_id <= maxTimeuuid(?)
"""

SELECT_ALL_SYMBOL_TRADES_FROM_HISTORY = """
    SELECT account, toDate(trade_id) AS Date, type, symbol, shares, price, amount
    FROM trades_by_a_sd
    WHERE account = ?
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_USERS_TABLE)
    session.execute(CREATE_POSSITIONS_BY_ACCOUNT_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_TRADE_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_SYMBOL_TRADE_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_SYMBOL_DATE_TABLE)


def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    print(tabulate(rows, headers=["Username", "Account Number", "Name", "Cash Balance"], tablefmt="rounded_grid"))

def get_account(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    accounts = session.execute(stmt, [username])
    accountNumbers = [[account.account_number] for account in accounts]
    if len(accountNumbers) > 1:
        print(tabulate(accountNumbers, headers=["Account"], tablefmt="rounded_grid", showindex="always"))
        option = int(input('Select your account (index): '))
        while option < 0 or option >= len(accountNumbers):
            print('Error, account not exist')
            option = int(input('Select your account (index): '))
        selectedAccount = str(f"{rows[option].account_number}")
    else:
        selectedAccount = str(f"{rows[0].account_number}")
    return selectedAccount

def get_type():
    TYPES = [['buy'], ['sell']]
    print(tabulate(TYPES, headers=["Types"], tablefmt="rounded_grid", showindex="always"))
    option = int(input('Select trade type (index): '))
    while option < 0 or option >= len(TYPES):
        print('Error, option not exist')
        option = int(input('Select trade type (index): '))
    typeSelected = str(f"{TYPES[option][0]}")
    return typeSelected

def get_symbol(session, account):
    stmt = session.prepare(SELECT_ALL_SYMBOL_TRADES_FROM_HISTORY) # para ejecutar un query lo necesito preparar ya que aqui se identifican los ? para cambiarlos por los parametros
    rows = session.execute(stmt, [account])
    INSTRUMENTS = set(account.symbol for account in rows)
    symbols = list(INSTRUMENTS)
    instrumentSymbols = [[instrument] for instrument in symbols]
    print(tabulate(instrumentSymbols, headers=["Instrument Symbols"], tablefmt="rounded_grid", showindex="always"))
    option = int(input('Select symbol trade (index): '))
    while option < 0 or option >= len(instrumentSymbols):
        print('Error, option not exist')
        option = int(input('Select symbol trade (index): '))
    symbolSelected = str(f"{instrumentSymbols[option][0]}")
    return symbolSelected

def get_user_position(session, account):
    log.info(f"Retrieving {account} accounts")
    stmt = session.prepare(SELECT_USER_POSITIONS) # para ejecutar un query lo necesito preparar
    rows = session.execute(stmt, [account]) # y despues ya puedo ejecutar ese query con esta linea de código
    print(tabulate(rows, headers=["Account", "Symbol", "Quantity"], tablefmt="rounded_grid"))

def optional_date(optional):
    if optional:
        return get_date_range()
    else:
        options = [["No"], ["Yes"]]
        print(tabulate(options, headers=["Option"], tablefmt="rounded_grid", showindex="always"))
        option = int(input('Date Range: '))
        while option < 0 or option >= len(options):
            print('Error, option not exist')
            option = int(input('Date Range: '))
        if option: 
            return get_date_range()

def get_date_range():
    startDateCorrect = 0
    while startDateCorrect != 1:
        start_date_str = input('Enter start date in YYYY-MM-DD format: ')
        try:
            # converte la cadena de fecha en un objeto de marca de tiempo
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            startDateCorrect = 1
        except ValueError:
            print("Invalid date format. Please enter the date in YYYY-MM-DD format.")
    endDateCorrect = 0
    while endDateCorrect != 1:
        end_date_str = input('Enter end date in YYYY-MM-DD format: ')
        try:
            # converte la cadena de fecha en un objeto de marca de tiempo
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            endDateCorrect = 1
        except ValueError:
            print("Invalid date format. Please enter the date in YYYY-MM-DD format.")
    return start_date, end_date

def get_all_trade_history(session, account):
    log.info(f"Retrieving {account} accounts")
    stmt = session.prepare(SELECT_ALL_TRADES_FROM_HISTORY) # para ejecutar un query lo necesito preparar
    rows = session.execute(stmt, [account]) # y despues ya puedo ejecutar ese query con esta linea de código
    print(tabulate(rows, headers=["Account", "Date", "Type", "Symbol", "Shares", "Price", "Amount"], tablefmt="rounded_grid"))

def get_date_range_trades_history(session, account, start_date, end_date):
    log.info(f"Retrieving {account} accounts")
    stmt = session.prepare(SELECT_DATE_RANGE_TRADES_FROM_HISTORY) # para ejecutar un query lo necesito preparar ya que aqui se identifican los ? para cambiarlos por los parametros
    rows = session.execute(stmt, [account, start_date, end_date]) # y despues ya puedo ejecutar ese query con esta linea de código, aqui se cambian los ? por los parametros de un arreglo
    print(tabulate(rows, headers=["Account", "Date", "Type", "Symbol", "Shares", "Price", "Amount"], tablefmt="rounded_grid"))

def get_type_trades_history(session, account, tradeType, dates):
    log.info(f"Retrieving {account} accounts")
    if dates == None:
        stmt = session.prepare(SELECT_TYPE_TRADES_FROM_HISTORY) # para ejecutar un query lo necesito preparar ya que aqui se identifican los ? para cambiarlos por los parametros
        rows = session.execute(stmt, [account, tradeType]) # y despues ya puedo ejecutar ese query con esta linea de código, aqui se cambian los ? por los parametros de un arreglo
    else:
        stmt = session.prepare(SELECT_TYPE_TRADES_FROM_HISTORY_BY_DATE) # para ejecutar un query lo necesito preparar ya que aqui se identifican los ? para cambiarlos por los parametros
        rows = session.execute(stmt, [account, tradeType, dates[0],dates[1]]) # y despues ya puedo ejecutar ese query con esta linea de código, aqui se cambian los ? por los parametros de un arreglo
    print(tabulate(rows, headers=["Account", "Date", "Type", "Symbol", "Shares", "Price", "Amount"], tablefmt="rounded_grid"))

def get_symbol_trades_history(session, account, symbolTrade, dates):
    log.info(f"Retrieving {account} accounts")
    if dates == None:
        stmt = session.prepare(SELECT_SYMBOL_TRADES_FROM_HISTORY) # para ejecutar un query lo necesito preparar ya que aqui se identifican los ? para cambiarlos por los parametros
        rows = session.execute(stmt, [account, symbolTrade]) # y despues ya puedo ejecutar ese query con esta linea de código, aqui se cambian los ? por los parametros de un arreglo
    else:
        stmt = session.prepare(SELECT_SYMBOL_TRADES_FROM_HISTORY_BY_DATE) # para ejecutar un query lo necesito preparar ya que aqui se identifican los ? para cambiarlos por los parametros
        rows = session.execute(stmt, [account, symbolTrade, dates[0], dates[1]]) # y despues ya puedo ejecutar ese query con esta linea de código, aqui se cambian los ? por los parametros de un arreglo
    print(tabulate(rows, headers=["Account", "Date", "Type", "Symbol", "Shares", "Price", "Amount"], tablefmt="rounded_grid"))
