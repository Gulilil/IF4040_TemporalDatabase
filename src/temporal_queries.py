import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    return psycopg2.connect(user=os.getenv("POSTGRE_DB_USER"), 
                            database=os.getenv("POSTGRE_DB_NAME"),
                            password=os.getenv("POSTGRE_DB_PASSWORD"),
                            host=os.getenv("POSTGRE_DB_HOST"),
                            port=os.getenv("POSTGRE_DB_PORT"))

### TEMPORAL OPERATIONS

# Temporal Projection
def temporal_projection(table, attributes, conditions=None):
    query = f"SELECT {', '.join(attributes)} FROM {table}"
    if conditions:
        query += f" WHERE {conditions}"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            print(f"Temporal Projection Result: {result}")
    return result

# Temporal Selection
def temporal_selection(table, predicate):
    query = f"SELECT * FROM {table} WHERE {predicate}"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            print(f"Temporal Selection Result: {result}")
    return result

# Temporal Union (restricted to s_id and p_id)
def temporal_union(table1, table2):
    query = f"SELECT s_id, p_id FROM {table1} UNION SELECT s_id, p_id FROM {table2}"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            print(f"Temporal Union Result: {result}")
    return result

# Temporal Difference (restricted to s_id and p_id)
def temporal_difference(table1, table2):
    query = f"SELECT s_id, p_id FROM {table1} EXCEPT SELECT s_id, p_id FROM {table2}"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            print(f"Temporal Difference Result: {result}")
    return result

# Temporal Join
def temporal_join(table1, table2, join_condition):
    query = f"""
        SELECT * 
        FROM {table1} AS t1
        JOIN {table2} AS t2 
        ON {join_condition}
        AND t1.vt_start < t2.vt_end AND t1.vt_end > t2.vt_start
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            print(f"Temporal Join Result: {result}")
    return result

# Timeslice
def timeslice(table, timestamp):
    query = f"""
        SELECT * 
        FROM {table} 
        WHERE vt_start <= '{timestamp}'::timestamp 
          AND vt_end > '{timestamp}'::timestamp
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            print(f"Timeslice Result: {result}")
    return result

# Main function to demonstrate usage
def main():
    try:
        # Example usage
        print("Temporal Projection:")
        temporal_projection("inventory_details", ["s_id", "p_id"], "region = 'North'")
        
        print("\nTemporal Selection:")
        temporal_selection("inventory_sales", "price > 100")

        print("\nTemporal Union:")
        temporal_union("inventory_details", "inventory_sales")

        print("\nTemporal Difference:")
        temporal_difference("inventory_details", "inventory_sales")

        print("\nTemporal Join:")
        temporal_join("inventory_details", "inventory_sales", "t1.s_id = t2.s_id AND t1.p_id = t2.p_id")

        print("\nTimeslice:")
        timeslice("inventory_details", "2025-01-01 00:00:00")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
