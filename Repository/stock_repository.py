import psycopg2
from psycopg2 import pool
import os
from dotenv import load_dotenv

# Get the parent directory and load the .env file from there
from pathlib import Path

# Get the parent directory
parent_dir = Path(__file__).resolve().parent.parent

# Load the .env file from the parent directory
dotenv_path = parent_dir / '.env'
load_dotenv(dotenv_path)

# Initialize a connection pool globally (size 1-10 connections)
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10, 
    user=os.getenv("DATABASE_USERNAME"), 
    password=os.getenv("DATABASE_PASSWORD"), 
    host=os.getenv("DATABASE_HOST"), 
    port=os.getenv("DATABASE_PORT"), 
    database=os.getenv("DATABASE_NAME")
)


def fetch_stock_from_db():
    """
    Function to retrieve all stocks from database
    
    Returns:
        dict: A dictionary representing all stocks.
    """

    # Get a connection from the pool
    conn = connection_pool.getconn()
  
    # create a cursor 
    cur = conn.cursor() 

    cur.execute('''SELECT * FROM stock''')
    data = cur.fetchall()

    # close connection
    cur.close()
    connection_pool.putconn(conn)  # return to the pool

    # Get column names from the cursor
    column_names = [desc[0] for desc in cur.description]

    # Convert the result into a list of dictionaries
    stock_result = [dict(zip(column_names, row)) for row in data]

    return stock_result

