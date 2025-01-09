import os
from dotenv import load_dotenv
import psycopg2
from allens_queries import (
    before_function,
    meets_function,
    overlaps_function,
    starts_function,
    during_function,
    finishes_function,
    equals_function,
)

# Load environment variables from .env file
load_dotenv()

# Function to establish connection to the database
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRE_DB_NAME"),
        user=os.getenv("POSTGRE_DB_USER"),
        password=os.getenv("POSTGRE_DB_PASSWORD"),
        host=os.getenv("POSTGRE_DB_HOST"),
        port=os.getenv("POSTGRE_DB_PORT")
    )

# Function to execute a query
def execute_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        connection.commit()

def drop_previous_functions(connection):
    functions_to_drop = [
        "before",
        "meets",
        "overlaps",
        "starts",
        "during",
        "finishes",
        "equals",
        "after",
        "met_by",
        "overlapped_by",
        "started_by",
        "contains",
        "finished_by",
    ]
    for func in functions_to_drop:
        query = f"DROP FUNCTION IF EXISTS {func}(TIMESTAMP, TIMESTAMP, TIMESTAMP, TIMESTAMP);"
        execute_query(connection, query)
        print(f"Function `{func}` dropped (if it existed).")

def create_allens_functions():
    connection = get_connection()
    try:

        print("Creating Allen's interval functions...")
        execute_query(connection, before_function)
        print("Function `before` created.")
        execute_query(connection, meets_function)
        print("Function `meets` created.")
        execute_query(connection, overlaps_function)
        print("Function `overlaps` created.")
        execute_query(connection, starts_function)
        print("Function `starts` created.")
        execute_query(connection, during_function)
        print("Function `during` created.")
        execute_query(connection, finishes_function)
        print("Function `finishes` created.")
        execute_query(connection, equals_function)
        print("Function `equals` created.")
        print("All functions created successfully.")
    finally:
        connection.close()

# Main function
if __name__ == "__main__":
    create_allens_functions()