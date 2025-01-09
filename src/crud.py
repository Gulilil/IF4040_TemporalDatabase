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


### QUERIES

# INSERT
def insert_inventory_details(s_id, p_id, category, region, seasonality, vt_start, vt_end='infinity', tt_start=None, tt_end='infinity'):
    query = """
        INSERT INTO inventory_details (
            s_id, p_id, category, region, seasonality, vt_start, vt_end, tt_start, tt_end
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
    """
    values = (s_id, p_id, category, region, seasonality, vt_start, vt_end, tt_start or 'NOW()', tt_end)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            new_id = cur.fetchone()[0]  # Get the ID of the inserted row
            conn.commit()
            print(f"Inserted into inventory_details with ID: {new_id}")

            # Verification
            cur.execute("SELECT * FROM inventory_details WHERE id = %s;", (new_id,))
            result = cur.fetchone()
            print(f"Verification Result: {result}")
    return new_id

def insert_inventory_sales(s_id, p_id, amount, units_sold, units_ordered, price, discount, refer_id, vt_start, vt_end='infinity', tt_start=None, tt_end='infinity'):
    query = """
        INSERT INTO inventory_sales (
            s_id, p_id, amount, units_sold, units_ordered, price, discount, refer_id, vt_start, vt_end, tt_start, tt_end
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
    """
    values = (s_id, p_id, amount, units_sold, units_ordered, price, discount, refer_id, vt_start, vt_end, tt_start or 'NOW()', tt_end)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            new_id = cur.fetchone()[0]  # Get the ID of the inserted row
            conn.commit()
            print(f"Inserted into inventory_sales with ID: {new_id}")

            # Verification
            cur.execute("SELECT * FROM inventory_sales WHERE id = %s;", (new_id,))
            result = cur.fetchone()
            print(f"Verification Result: {result}")
    return new_id

# UPDATE
def update_inventory_details(id, category=None, region=None, vt_end=None):
    query = "UPDATE inventory_details SET "
    updates = []
    params = []

    if category:
        updates.append("category = %s")
        params.append(category)
    if region:
        updates.append("region = %s")
        params.append(region)
    if vt_end:
        updates.append("vt_end = %s")
        params.append(vt_end)
    
    query += ", ".join(updates) + " WHERE id = %s;"
    params.append(id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            conn.commit()
            print(f"Updated inventory_details with ID: {id}")

            # Verification
            cur.execute("SELECT * FROM inventory_details WHERE id = %s;", (id,))
            result = cur.fetchone()
            print(f"Verification Result: {result}")

def update_inventory_sales(id, amount=None, units_sold=None, discount=None):
    query = "UPDATE inventory_sales SET "
    updates = []
    params = []

    if amount is not None:
        updates.append("amount = %s")
        params.append(amount)
    if units_sold is not None:
        updates.append("units_sold = %s")
        params.append(units_sold)
    if discount is not None:
        updates.append("discount = %s")
        params.append(discount)
    
    query += ", ".join(updates) + " WHERE id = %s;"
    params.append(id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            conn.commit()
            print(f"Updated inventory_sales with ID: {id}")

            # Verification
            cur.execute("SELECT * FROM inventory_sales WHERE id = %s;", (id,))
            result = cur.fetchone()
            print(f"Verification Result: {result}")

# DELETE
def delete_inventory_details(id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Check existence before deletion
            cur.execute("SELECT * FROM inventory_details WHERE id = %s;", (id,))
            exists = cur.fetchone()
            if not exists:
                print(f"No record found in inventory_details with ID: {id}")
                return

            # Perform deletion
            cur.execute("DELETE FROM inventory_details WHERE id = %s;", (id,))
            conn.commit()
            print(f"Deleted from inventory_details with ID: {id}")

            # Verification
            cur.execute("SELECT * FROM inventory_details WHERE id = %s;", (id,))
            result = cur.fetchone()
            print(f"Verification Result: {result}")

def delete_inventory_sales(id):
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Check existence before deletion
            cur.execute("SELECT * FROM inventory_sales WHERE id = %s;", (id,))
            exists = cur.fetchone()
            if not exists:
                print(f"No record found in inventory_sales with ID: {id}")
                return

            # Perform deletion
            cur.execute("DELETE FROM inventory_sales WHERE id = %s;", (id,))
            conn.commit()
            print(f"Deleted from inventory_sales with ID: {id}")

            # Verification
            cur.execute("SELECT * FROM inventory_sales WHERE id = %s;", (id,))
            result = cur.fetchone()
            print(f"Verification Result: {result}")

# For setting the values
def reset_sequence(table_name, column_name):
    """Resets the sequence for a table's SERIAL column to align with the max ID."""
    query = f"""
        SELECT setval(pg_get_serial_sequence('{table_name}', '{column_name}'), COALESCE(MAX({column_name}), 1)) 
        FROM {table_name};
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
    print(f"Sequence for {table_name}.{column_name} has been reset.")

# Main function to demonstrate usage
def main():
    try:
        # Reset sequences before any operation
        reset_sequence('inventory_details', 'id')
        reset_sequence('inventory_sales', 'id')

        # Insert into inventory_details
        details_id = insert_inventory_details(
            s_id="S001",
            p_id="P001",
            category="Electronics",
            region="North",
            seasonality="Winter",
            vt_start="2025-01-01 00:00:00"
        )
        
        # Insert into inventory_sales
        sales_id = insert_inventory_sales(
            s_id="S001",
            p_id="P001",
            amount=5000,
            units_sold=10,
            units_ordered=15,
            price=499.99,
            discount=10,
            refer_id=details_id,
            vt_start="2025-01-01 00:00:00"
        )

        # Update inventory_details
        update_inventory_details(
            id=details_id,
            category="Home Appliances",
            region="East",
            vt_end="2025-12-31 23:59:59"
        )

        # Update inventory_sales
        update_inventory_sales(
            id=sales_id,
            amount=6000,
            units_sold=12,
            discount=15
        )

        # Delete from inventory_sales
        delete_inventory_sales(id=sales_id)

        # Delete from inventory_details
        delete_inventory_details(id=details_id)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
