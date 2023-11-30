import os
import pandas as pd
import uuid

DATA = os.path.abspath(os.path.join(os.path.dirname(__file__), '../flight_passengers.csv'))

def cql_generator():
    fligth_by_tr_m = "INSERT INTO flight_passengers_tr_m (passenger_id, airline, from_loc, to_loc, day, month, year, age, gender, reason, stay, transit, connection, wait, price) VALUES ('{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', '{}', '{}', {}, {}, {});"
    fligth_by_rm = "INSERT INTO flight_passengers_rm (passenger_id, airline, from_loc, to_loc, day, month, year, age, gender, reason, stay, transit, connection, wait, price) VALUES ('{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', '{}', '{}', {}, {}, {});"

    data = pd.read_csv(DATA)

    file_name = os.path.join(os.path.dirname(__file__), "CASSANDRA_DATA.cql")

    with open(file_name, 'w+') as file:
        for index, row in data.iterrows():
            cql_statement_tr_m = fligth_by_tr_m.format(
                str(uuid.uuid4()),
                row['airline'], row['from_loc'], row['to_loc'], int(row['day']), int(row['month']),
                int(row['year']), int(row['age']), row['gender'], row['reason'], row['stay'],
                row['transit'], row['connection'], int(row['wait']), float(row['price'])
            )
            cql_statement_rm = fligth_by_rm.format(
                str(uuid.uuid4()),
                row['airline'], row['from_loc'], row['to_loc'], int(row['day']), int(row['month']),
                int(row['year']), int(row['age']), row['gender'], row['reason'], row['stay'],
                row['transit'], row['connection'], int(row['wait']), float(row['price'])
            )

            file.write(cql_statement_tr_m + '\n')
            file.write(cql_statement_rm + '\n\n')

cql_generator()
