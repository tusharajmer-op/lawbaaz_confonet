import psycopg2 as pg
import json
import config
from datetime import datetime, date

# Function to establish a connection to the PostgreSQL database
def get_db_conn():
    """
    Connects to the specified database and returns the connection object.
    """
    conn = pg.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASS,
        database=config.DB_NAME)
    return conn

# Function to perform database operations
def db_operations(query, isread=True, return_data=False):
    """
    Perform database operations based on the provided query.
    """
    conn = get_db_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if isread:
            data = cursor.fetchall()
            if return_data:
                return True, data
            else:
                return True,[]
        else:
            conn.commit()
            return True,[]
    except Exception as error:
        conn.rollback()
        print("Error while connecting to PostgreSQL:", error)
        return False, str(error)
    finally:
        cursor.close()
        conn.close()
