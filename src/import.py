from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import time
from queries import CREATE_TABLE_INVENTORY_DETAILS, CREATE_TABLE_INVENTORY_SALES

load_dotenv()

DATA_DIR = os.path.join(os.getcwd(), "data")

def execute_query(query: str):
  conn = None
  cur = None
  try:
    conn = psycopg2.connect(user=os.getenv("POSTGRE_DB_USER"), 
                            database=os.getenv("POSTGRE_DB_NAME"),
                            password=os.getenv("POSTGRE_DB_PASSWORD"),
                            host=os.getenv("POSTGRE_DB_HOST"),
                            port=os.getenv("POSTGRE_DB_PORT"))
    cur = conn.cursor()
    # print(f"[CONNECTED] Connected to database: {os.getenv('POSTGRE_DB_NAME')}")

  except Exception as e: 
    print(f"[CONNECTION FAILED] Failed to connect to database: {os.getenv('POSTGRE_DB_NAME')} : {e}")

  if (conn is not None and cur is not None):
    try:
      cur.execute(query)
      conn.commit()
    except Exception as e:
      print(f"[FAILED] Failed to execute query {query[:100]} ... {query[-100:]} : {e}")
    finally:
      cur.close()
      conn.close()

def make_string(text: str, quote : str = "\'"):
  return f"{quote}{text}{quote}"

def insert_details(df):
  i = 0
  max_batch = 1000
  while (i <= len(df)):
    bound = min(len(df), i + max_batch)
    partial_df = df[i:bound]
    query = "INSERT INTO inventory_details VALUES \n"
    for _, row in partial_df.iterrows():
      row_query = "("
      row_query += str(row['id']) + ", "
      row_query += make_string(row['s_id']) + ", "
      row_query += make_string(row['p_id']) + ", "
      row_query += make_string(row['category']) + ", "
      row_query += make_string(row['region']) + ", "
      row_query += make_string(row['seasonality']) + ", "
      row_query += make_string(row['vt_start']) + ", "
      row_query += make_string(row['vt_end']) + ", "
      row_query += make_string(row['tt_start']) + ", "
      row_query += make_string(row['tt_end'])
      row_query += "),\n"
      query += row_query 
    query = query[:-2] +";"
    execute_query(query)
    print(f"[INSERTED] Insert {i} data")
    i += max_batch
    time.sleep(0.1)

def insert_sales(df):
  i = 0
  max_batch = 1000
  while (i <= len(df)):
    bound = min(len(df), i + max_batch)
    partial_df = df[i:bound]
    query = "INSERT INTO inventory_sales VALUES \n"
    for _, row in partial_df.iterrows():
      row_query = "("
      row_query += str(row['id']) + ", "
      row_query += make_string(row['s_id']) + ", "
      row_query += make_string(row['p_id']) + ", "
      row_query += str(row['amount']) + ", "
      row_query += str(row['units_sold'])+ ", "
      row_query += str(row['units_ordered']) + ", "
      row_query += str(row['price']) + ", "
      row_query += str(row['discount']) + ", "
      row_query += str(row['refer_id']) + ", "
      row_query += make_string(row['vt_start']) + ", "
      row_query += make_string(row['vt_end']) + ", "
      row_query += make_string(row['tt_start']) + ", "
      row_query += make_string(row['tt_end'])
      row_query += "),\n"
      query += row_query 
    query = query[:-2] +";"
    execute_query(query)
    print(f"[INSERTED] Insert {i} data")
    i += max_batch
    time.sleep(0.1)

if __name__ == "__main__":
  conn = None
  cur = None
  try:
    conn = psycopg2.connect(user=os.getenv("POSTGRE_DB_USER"), 
                            database=os.getenv("POSTGRE_DB_NAME"),
                            password=os.getenv("POSTGRE_DB_PASSWORD"),
                            host=os.getenv("POSTGRE_DB_HOST"),
                            port=os.getenv("POSTGRE_DB_PORT"))
    cur = conn.cursor()

    conn.close()
    cur.close()

    # Create Tables
    execute_query(CREATE_TABLE_INVENTORY_DETAILS)
    execute_query(CREATE_TABLE_INVENTORY_SALES)
    print(f"[SUCCESS] Successfully create tables")

    df_details = pd.read_csv(os.path.join(DATA_DIR, "inventory_details.csv"))
    df_sales = pd.read_csv(os.path.join(DATA_DIR, "inventory_sales.csv"))

    insert_details(df_details)
    insert_sales(df_sales)

  except Exception as e: 
    print(f"[CONNECTION FAILED] Failed to connect to database: {os.getenv('POSTGRE_DB_NAME')} : {e}")