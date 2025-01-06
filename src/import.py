from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

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