import os
from dotenv import load_dotenv
import psycopg2

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

def create_allens_interval_functions(connection):
    functions = {
        "before": """
        CREATE OR REPLACE FUNCTION before(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN end1 < start2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "meets": """
        CREATE OR REPLACE FUNCTION meets(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN end1 = start2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "overlaps": """
        CREATE OR REPLACE FUNCTION overlaps(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start1 < start2 AND end1 > start2 AND end1 < end2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "starts": """
        CREATE OR REPLACE FUNCTION starts(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start1 = start2 AND end1 < end2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "during": """
        CREATE OR REPLACE FUNCTION during(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start1 > start2 AND end1 < end2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "finishes": """
        CREATE OR REPLACE FUNCTION finishes(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN end1 = end2 AND start1 > start2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "equals": """
        CREATE OR REPLACE FUNCTION equals(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start1 = start2 AND end1 = end2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "after": """
        CREATE OR REPLACE FUNCTION after(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start1 > end2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "met_by": """
        CREATE OR REPLACE FUNCTION met_by(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start1 = end2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "overlapped_by": """
        CREATE OR REPLACE FUNCTION overlapped_by(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start2 < start1 AND end2 > start1 AND end2 < end1;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "started_by": """
        CREATE OR REPLACE FUNCTION started_by(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start1 = start2 AND end1 > end2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "contains": """
        CREATE OR REPLACE FUNCTION contains(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN start1 < start2 AND end1 > end2;
        END;
        $$ LANGUAGE plpgsql;
        """,

        "finished_by": """
        CREATE OR REPLACE FUNCTION finished_by(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
        RETURNS BOOLEAN AS $$
        BEGIN
            RETURN end1 = end2 AND start1 < start2;
        END;
        $$ LANGUAGE plpgsql;
        """
    }

    for name, query in functions.items():
        execute_query(connection, query)
        print(f"Function '{name}' created successfully.")

def main():
    connection = get_connection()
    try:
        create_allens_interval_functions(connection)
    finally:
        connection.close()

if __name__ == "__main__":
    main()

