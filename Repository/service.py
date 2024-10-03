import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool

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
