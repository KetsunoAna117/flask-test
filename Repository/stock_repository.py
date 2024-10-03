from Repository.service import connection_pool

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

