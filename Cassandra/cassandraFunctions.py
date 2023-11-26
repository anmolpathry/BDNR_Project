import os
import pandas as pd

DATA = os.path.abspath(os.path.join(os.path.dirname(__file__), '../flight_passengers.csv'))

def cql_generator():
    fligth_by_d = "INSERT INTO flight_passengers_d (airline, from, to, day, month, year, age, gender, reason, stay, transit, connection, wait, price) VALUES ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}');"
    fligth_by_g_d = "INSERT INTO flight_passengers_g_d (airline, from, to, day, month, year, age, gender, reason, stay, transit, connection, wait, price) VALUES ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}');"
    fligth_by_ai_d = "INSERT INTO flight_passengers_ai_d (airline, from, to, day, month, year, age, gender, reason, stay, transit, connection, wait, price) VALUES ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}');"
    fligth_by_f_d = "INSERT INTO flight_passengers_f_d (airline, from, to, day, month, year, age, gender, reason, stay, transit, connection, wait, price) VALUES ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}', {}, '{}', '{}', '{}');"

    data = pd.read_csv(DATA)

    file_name = "CASSANDRA_DATA.cql"

    with open(file_name, 'w+') as file:
        for index, row in data.iterrows():
            cql_statement_d = fligth_by_d.format(
                row['airline'], row['from'], row['to'], row['day'], row['month'],
                row['year'], row['age'], row['gender'], row['reason'], row['stay'],
                row['transit'], row['connection'], row['wait'], row['price']
            )
            cql_statement_g_d = fligth_by_g_d.format(
                row['airline'], row['from'], row['to'], row['day'], row['month'],
                row['year'], row['age'], row['gender'], row['reason'], row['stay'],
                row['transit'], row['connection'], row['wait'], row['price']
            )
            cql_statement_ai_d = fligth_by_ai_d.format(
                row['airline'], row['from'], row['to'], row['day'], row['month'],
                row['year'], row['age'], row['gender'], row['reason'], row['stay'],
                row['transit'], row['connection'], row['wait'], row['price']
            )
            cql_statement_f_d = fligth_by_f_d.format(
                row['airline'], row['from'], row['to'], row['day'], row['month'],
                row['year'], row['age'], row['gender'], row['reason'], row['stay'],
                row['transit'], row['connection'], row['wait'], row['price']
            )

            file.write(cql_statement_d + ' ')
            file.write(cql_statement_g_d + ' ')
            file.write(cql_statement_ai_d + ' ')
            file.write(cql_statement_f_d + '\n')

cql_generator()
