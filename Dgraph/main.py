from pydgraph import DgraphClient, DgraphClientStub
import os
import model
import populate

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def print_menu():
    mm_options = {
        1: "Create data",
        8: "Drop All",
        0: "Exit"
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def create_client_stub():
    return DgraphClientStub(DGRAPH_URI)

def create_client(client_stub):
    return DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

def main():
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Create schema
    model.set_schema(client)

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            populate.create_data(client)
        if option == 8:
            model.drop_all(client)
        if option == 0:
            close_client_stub(client_stub)
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))