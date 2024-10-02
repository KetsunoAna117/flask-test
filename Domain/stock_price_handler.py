from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import pool

# Get the parent directory and load the .env file from there
from pathlib import Path

# Get the parent directory
parent_dir = Path(__file__).resolve().parent.parent

# Load the .env file from the parent directory
dotenv_path = parent_dir / '.env'
load_dotenv(dotenv_path)

# Temp function to fetch news
# This function will be replaced with actual implementation
def fetch_news(connection_pool):
    # Get a connection from the pool
    conn = connection_pool.getconn()

    # create a cursor 
    cur = conn.cursor() 

    cur.execute('''SELECT * FROM news''')
    data = cur.fetchall()

    # close connection
    cur.close()
    connection_pool.putconn(conn)  # return to the pool

    print(data[0])

# This function will change the stock price periodically based on the news
def change_stock_price(connection_pool, socketIO, news):
     # Get a connection from the pool
    conn = connection_pool.getconn()
  
    # create a cursor 
    cur = conn.cursor() 

    cur.execute('''SELECT * FROM stock WHERE stock_id = %s''', (news['id'],))
    stock = cur.fetchall()

    # close connection
    cur.close()
    connection_pool.putconn(conn)  # return to the pool

    return stock