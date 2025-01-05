import psycopg2

# Function to establish connection to the database
def get_connection():
    return psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="localhost",
        port=5432
    )

# Function to execute Allen's relationship queries
def execute_allens_relationship_query(connection, query, params=None):
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()

# Function to compute Allen's 13 temporal relationships
def get_allens_relationship(connection, table, range1, range2):
    relationships = {
        "before": f"SELECT * FROM {table} WHERE {range1} << {range2}",
        "meets": f"SELECT * FROM {table} WHERE upper({range1}) = lower({range2})",
        "overlaps": f"SELECT * FROM {table} WHERE {range1} && {range2} AND NOT ({range1} << {range2} OR {range1} >> {range2})",
        "starts": f"SELECT * FROM {table} WHERE lower({range1}) = lower({range2}) AND upper({range1}) < upper({range2})",
        "during": f"SELECT * FROM {table} WHERE {range1} <@ {range2} AND NOT {range1} = {range2}",
        "finishes": f"SELECT * FROM {table} WHERE upper({range1}) = upper({range2}) AND lower({range1}) > lower({range2})",
        "equals": f"SELECT * FROM {table} WHERE {range1} = {range2}",
        "after": f"SELECT * FROM {table} WHERE {range1} >> {range2}",
        "met_by": f"SELECT * FROM {table} WHERE lower({range1}) = upper({range2})",
        "overlapped_by": f"SELECT * FROM {table} WHERE {range1} && {range2} AND NOT ({range1} << {range2} OR {range1} >> {range2})",
        "started_by": f"SELECT * FROM {table} WHERE lower({range1}) = lower({range2}) AND upper({range1}) > upper({range2})",
        "contains": f"SELECT * FROM {table} WHERE {range2} <@ {range1} AND NOT {range1} = {range2}",
        "finished_by": f"SELECT * FROM {table} WHERE upper({range1}) = upper({range2}) AND lower({range1}) < lower({range2})"
    }
    results = {}
    for rel, query in relationships.items():
        results[rel] = execute_allens_relationship_query(connection, query)
    return results

# Function to coalesce temporal data
def coalesce_temporal_data(connection, table, column, default_value):
    query = f"""
    SELECT id, COALESCE({column}, %s) AS coalesced_value
    FROM {table}
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (default_value,))
        return cursor.fetchall()

# Main function to demonstrate usage
def main():
    # Connect to the database
    connection = get_connection()
    
    try:
        # Example: Find Allen's relationships for a specific range
        range1 = 'valid_time'
        range2 = "'[2023-02-01, 2023-03-15]'::tsrange"
        relationships = get_allens_relationship(connection, 'temporal_data', range1, range2)
        
        print("Allen's Relationships:")
        for rel, rows in relationships.items():
            print(f"{rel}: {rows}")
        
        # Example: Coalesce values in temporal_data table
        default_value = 'Default Event'
        coalesced_data = coalesce_temporal_data(connection, 'temporal_data', 'value', default_value)
        
        print("\nCoalesced Data:")
        for row in coalesced_data:
            print(row)
    
    finally:
        connection.close()

if __name__ == "__main__":
    main()
