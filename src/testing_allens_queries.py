import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd

load_dotenv()

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRE_DB_NAME"),
        user=os.getenv("POSTGRE_DB_USER"),
        password=os.getenv("POSTGRE_DB_PASSWORD"),
        host=os.getenv("POSTGRE_DB_HOST"),
        port=os.getenv("POSTGRE_DB_PORT")
    )

def execute_query(connection, query, params=None):
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()

# Function to compute Allen's 13 temporal relationships 
def get_allens_relationship_row_comparison(connection, table):
    relationships = {
        "before": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE before(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "meets": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE meets(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "overlaps": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE overlaps(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "starts": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE starts(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "during": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE during(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "finishes": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE finishes(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "equals": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE equals(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "after": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE after(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "met_by": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE met_by(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "overlapped_by": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE overlapped_by(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "started_by": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE started_by(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "contains": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE contains(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10",
        "finished_by": f"SELECT t1.*, t2.* FROM {table} t1, {table} t2 WHERE finished_by(t1.vt_start, t1.vt_end, t2.vt_start, t2.vt_end) LIMIT 10"
    }
    results = {}
    for rel, query in relationships.items():
        results[rel] = execute_query(connection, query)
    return results

def summarize_allens_relationship(relationships):
    summary = {}
    for rel, rows in relationships.items():
        summary[rel] = {
            "count": len(rows),
            "examples": rows[:5]
        }
    return summary

def print_relationship_summary(summary):
    for rel, data in summary.items():
        print(f"\n[RELATIONSHIP: {rel.upper()}]")
        print(f"Count: {data['count']} pairs")
        print("Examples:")
        if data['examples']:
            df = pd.DataFrame(data['examples'], columns=["id1", "s_id1", "p_id1", "category1", "region1", "seasonality1", "vt_start1", "vt_end1", "tt_start1", "tt_end1", 
                                                         "id2", "s_id2", "p_id2", "category2", "region2", "seasonality2", "vt_start2", "vt_end2", "tt_start2", "tt_end2"])
            print(df.to_string(index=False))
        else:
            print("No records found for this relationship.")

def main():
    connection = get_connection()
    
    try:
        # Example: Allen's relationships for `inventory_details`
        relationships_details = get_allens_relationship_row_comparison(connection, 'inventory_details')
        
        # Summarize and print relationships
        summary_details = summarize_allens_relationship(relationships_details)
        print("Summary of Allen's Relationships for `inventory_details` (row vs row):")
        print_relationship_summary(summary_details)

    finally:
        connection.close()

if __name__ == "__main__":
    main()
