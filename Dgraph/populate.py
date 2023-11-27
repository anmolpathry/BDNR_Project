import csv
import os
import pydgraph

DATA = os.path.abspath(os.path.join(os.path.dirname(__file__), '../flight_passengers.csv'))

def create_data(client):
    # Read data from CSV file and create mutation
    with open(DATA) as csvfile:
        reader = csv.DictReader(csvfile)
        nodes = []

        for row in reader:
            # Generate unique UIDs for each node
            uid_from = f"_:{row['from_loc']}"
            uid_to = f"_:{row['to_loc']}"
            uid_airline = f"_:{row['airline']}"
            uid_person = f"_:{row['age']}{row['gender']}{row['stay']}"
            uid_flight = f"_:{row['day']}{row['month']}{row['year']}{row['price']}"

            # Create nodes for airports
            to_node = {'uid': uid_to, 'airport_name': row['to_loc']}
            from_node = {'uid': uid_from, 'airport_name': row['from_loc'], 'to_loc_airport':to_node}

            # Create node for airline
            airline_node = {'uid': uid_airline, 'airline_name': row['airline']}

            # Create node for person
            person_node = {
                'uid': uid_person,
                'age': int(row['age']),
                'gender': row['gender'],
                'reason': row['reason'],
                'stay': row['stay']
            }

            # Create flight node
            flight_node = {
                'uid': uid_flight,
                'day': int(row['day']),
                'month': int(row['month']),
                'year': int(row['year']),
                'connection': row['connection'].lower() == 'true',
                'wait': int(row['wait']),
                'price': int(row['price']),
                'from_loc': from_node,
                'to_loc': to_node,
                'airline': airline_node,
                'passenger': person_node
            }

            nodes.extend([from_node, to_node, airline_node, person_node, flight_node])

    txn = client.txn()
    try:
        #for node in nodes:
        print(type(nodes))
        print(nodes)
        response = txn.mutate(set_obj=nodes)
        # Commit transaction.
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")

        print(f"UIDs: {response.uids}")
        print('Data imported successfully!')
    finally:
        txn.discard()
