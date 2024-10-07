from Repository.service import connection_pool

def create_stock_price_detail(stock_id, stock_date, stock_price, stock_price_time):
    """
    Function to create a new row in the stock_price_detail table using stock_id and stock_date.

    Args:
        stock_id (int): The ID of the stock.
        stock_date (int): The stock day to fetch the stock_detail_id.
        stock_price (int): The price to insert.
        stock_price_time (str): The time of the price (in HH:MM:SS format).

    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    
    # Import the necessary function that is needed to be called
    from Repository.stock_detail_repository import fetch_stock_detail_id_by_stock_id_and_date
    
    # Fetch the stock_detail_id based on stock_id and stock_date
    stock_detail_id = fetch_stock_detail_id_by_stock_id_and_date(stock_id, stock_date)
    
    if stock_detail_id is None:
        print(f"Error: No stock_detail found for stock_id {stock_id} on day {stock_date}.")
        return False  # Cannot insert if no stock_detail_id found
    
    # Proceed to insert into stock_price_detail using stock_detail_id
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # SQL query to insert a new row into the stock_price_detail table
        query = '''
        INSERT INTO stock_price_detail (stock_detail_id, stock_price, stock_price_time)
        VALUES (%s, %s, %s);
        '''
        
        # Execute the query with the fetched stock_detail_id
        cur.execute(query, (stock_detail_id, stock_price, stock_price_time))
        
        # Commit the transaction
        conn.commit()
        
        print(f"New stock price detail added successfully with stock_detail_id {stock_detail_id}.")
        return True  # Insertion was successful
    
    except Exception as e:
        print(f"Error inserting stock price detail: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Insertion failed
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def fetch_stock_price_detail_by_stock_price_detail_id(stock_price_detail_id):
    """
    Function to retrieve a specific stock price detail by stock_price_detail_id from the 'stock_price_detail' table.

    Args:
        stock_price_detail_id (int): The ID of the stock price detail to fetch.

    Returns:
        dict: A dictionary representing the stock price detail item if found, otherwise None.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # SQL query to fetch stock price detail by its ID
        query = '''
        SELECT * FROM stock_price_detail
        WHERE stock_price_detail_id = %s;
        '''
        
        # Execute the query to fetch the specific stock price detail item
        cur.execute(query, (stock_price_detail_id,))
        price_detail_data = cur.fetchone()
        
        # If no stock price detail is found, return None
        if not price_detail_data:
            print(f"No stock price detail found with stock_price_detail_id {stock_price_detail_id}")
            return None
        
        # Get column names from the cursor
        column_names = [desc[0] for desc in cur.description]
        
        # Convert the result into a dictionary
        price_detail_result = dict(zip(column_names, price_detail_data))
        
        return price_detail_result
    
    except Exception as e:
        print(f"Error fetching stock price detail with stock_price_detail_id {stock_price_detail_id} from database: {e}")
        return None
    
    finally:
        # Ensure that the cursor is closed and the connection is returned to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_all_prices_by_stock_detail_id(stock_detail_id):
    """
    Function to retrieve all prices and price times from stock_price_detail 
    based on the stock_detail_id.

    Args:
        stock_detail_id (int): The detail ID of the stock.

    Returns:
        list: A list of dictionaries representing each stock_price and stock_price_time.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # SQL query to fetch all prices and price times based on stock_id and stock_date
        query = '''
        SELECT * FROM stock_price_detail
        WHERE stock_detail_id = %s;
        '''
        
        # Execute the query
        cur.execute(query, (stock_detail_id))
        price_data = cur.fetchall()
        
        # Get column names from the cursor
        column_names = [desc[0] for desc in cur.description]
        
        # Convert the result into a list of dictionaries
        price_result = [dict(zip(column_names, row)) for row in price_data]
        
        return price_result
    
    except Exception as e:
        print(f"Error fetching prices for stock_detail_id {stock_detail_id}: {e}")
        return []
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_all_prices_by_stock_id_and_date(stock_id, stock_date):
    """
    Function to retrieve all prices and price times from stock_price_detail 
    based on the stock_id and stock_date.

    Args:
        stock_id (int): The ID of the stock.
        stock_date (int): The stock day for which to fetch the prices and price times.

    Returns:
        list: A list of dictionaries representing each stock_price and stock_price_time.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # SQL query to fetch all prices and price times based on stock_id and stock_date
        query = '''
        SELECT spd.stock_price, spd.stock_price_time
        FROM stock_price_detail spd
        JOIN stock_detail sd ON spd.stock_detail_id = sd.stock_detail_id
        WHERE sd.stock_id = %s AND sd.stock_date = %s;
        '''
        
        # Execute the query
        cur.execute(query, (stock_id, stock_date))
        price_data = cur.fetchall()
        
        # Get column names from the cursor
        column_names = [desc[0] for desc in cur.description]
        
        # Convert the result into a list of dictionaries
        price_result = [dict(zip(column_names, row)) for row in price_data]
        
        return price_result
    
    except Exception as e:
        print(f"Error fetching prices for stock_id {stock_id} on day {stock_date}: {e}")
        return []
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_last_price(stock_id, stock_date):
    """
    Function to fetch the last price for a specific stock_id and stock_date.
    
    Args:
        stock_id (int): The ID of the stock to fetch the price for.
        stock_date (int): The specific stock day to fetch the price for.
    
    Returns:
        int: The last price for the specific stock day, or 0 if no price was found.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # Query to fetch the last price for the given stock_id and stock_date
        query = '''
        SELECT spd.stock_price
        FROM stock_price_detail spd
        JOIN stock_detail sd ON spd.stock_detail_id = sd.stock_detail_id
        WHERE sd.stock_id = %s AND sd.stock_date = %s
        ORDER BY spd.stock_price_time DESC
        LIMIT 1;
        '''
        
        # Execute the query to fetch the last price
        cur.execute(query, (stock_id, stock_date))
        last_price_result = cur.fetchone()

        if last_price_result:
            return last_price_result[0]  # Return the last price for the specific stock day
        else:
            return 0  # If no price found, return 0
    
    except Exception as e:
        print(f"Error fetching last price for stock_id {stock_id} on day {stock_date}: {e}")
        return 0  # Default to 0 in case of an error
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_highest_price(stock_id, stock_date):
    """
    Function to fetch the highest price for a specific stock_id and stock_date.

    Args:
        stock_id (int): The ID of the stock to fetch the highest price for.
        stock_date (int): The stock day to fetch the highest price for.

    Returns:
        int: The highest price for the specific stock day, or 0 if no price is found.
    """
    
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # Query to fetch the highest price
        query = '''
        SELECT MAX(spd.stock_price)
        FROM stock_price_detail spd
        JOIN stock_detail sd ON spd.stock_detail_id = sd.stock_detail_id
        WHERE sd.stock_id = %s AND sd.stock_date = %s;
        '''
        
        cur.execute(query, (stock_id, stock_date))
        highest_price_result = cur.fetchone()
        
        if highest_price_result:
            return highest_price_result[0]
        else:
            return 0
    
    except Exception as e:
        print(f"Error fetching highest price for stock_id {stock_id} on day {stock_date}: {e}")
        return 0
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def fetch_lowest_price(stock_id, stock_date):
    """
    Function to fetch the lowest price for a specific stock_id and stock_date.

    Args:
        stock_id (int): The ID of the stock to fetch the lowest price for.
        stock_date (int): The stock day to fetch the lowest price for.

    Returns:
        int: The lowest price for the specific stock day, or 0 if no price is found.
    """
    
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # Query to fetch the lowest price
        query = '''
        SELECT MIN(spd.stock_price)
        FROM stock_price_detail spd
        JOIN stock_detail sd ON spd.stock_detail_id = sd.stock_detail_id
        WHERE sd.stock_id = %s AND sd.stock_date = %s;
        '''
        
        cur.execute(query, (stock_id, stock_date))
        lowest_price_result = cur.fetchone()
        
        if lowest_price_result:
            return lowest_price_result[0]
        else:
            return 0
    
    except Exception as e:
        print(f"Error fetching lowest price for stock_id {stock_id} on day {stock_date}: {e}")
        return 0
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def update_stock_price_detail_by_stock_detail_id(stock_detail_id, stock_price, stock_price_time):
    """
    Function to update a row in the stock_price_detail table with the given stock_price and stock_price_time
    by the stock_detail_id.

    Args:
        stock_detail_id (int): The detail ID of the stock.
        stock_price (int): The price to update.
        stock_price_time (str): The time of the price (in HH:MM:SS format).

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    
    if stock_detail_id is None:
        print(f"Error: No stock_detail found for stock_detail_id {stock_detail_id}.")
        return False  # Cannot update if no stock_detail_id found
    
    # Proceed to update the price in stock_price_detail using stock_detail_id
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # SQL query to update the price in stock_price_detail for the given stock_detail_id and stock_price_time
        query = '''
        UPDATE stock_price_detail
        SET stock_price = %s
        WHERE stock_detail_id = %s AND stock_price_time = %s;
        '''
        
        # Execute the query with the provided parameters
        cur.execute(query, (stock_price, stock_detail_id, stock_price_time))
        
        # Commit the transaction
        conn.commit()
        
        print(f"Stock price detail updated successfully for stock_detail_id {stock_detail_id} at {stock_price_time}.")
        return True  # Update was successful
    
    except Exception as e:
        print(f"Error updating stock price detail: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Update failed
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def update_stock_price_detail_by_stock_id_and_date(stock_id, stock_date, stock_price, stock_price_time):
    """
    Function to update a row in the stock_price_detail table with the given stock_price and stock_price_time
    by first fetching the stock_detail_id using stock_id and stock_date.

    Args:
        stock_id (int): The ID of the stock.
        stock_date (int): The stock day for which to fetch the stock_detail_id.
        stock_price (int): The price to update.
        stock_price_time (str): The time of the price (in HH:MM:SS format).

    Returns:
        bool: True if the update was successful, False otherwise.
    """

    # Import the necessary function that is needed to be called
    from Repository.stock_detail_repository import fetch_stock_detail_id_by_stock_id_and_date
    
    # Fetch the stock_detail_id based on stock_id and stock_date
    stock_detail_id = fetch_stock_detail_id_by_stock_id_and_date(stock_id, stock_date)
    
    if stock_detail_id is None:
        print(f"Error: No stock_detail found for stock_id {stock_id} on day {stock_date}.")
        return False  # Cannot update if no stock_detail_id found
    
    # Proceed to update the price in stock_price_detail using stock_detail_id
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # SQL query to update the price in stock_price_detail for the given stock_detail_id and stock_price_time
        query = '''
        UPDATE stock_price_detail
        SET stock_price = %s
        WHERE stock_detail_id = %s AND stock_price_time = %s;
        '''
        
        # Execute the query with the provided parameters
        cur.execute(query, (stock_price, stock_detail_id, stock_price_time))
        
        # Commit the transaction
        conn.commit()
        
        print(f"Stock price detail updated successfully for stock_detail_id {stock_detail_id} at {stock_price_time}.")
        return True  # Update was successful
    
    except Exception as e:
        print(f"Error updating stock price detail: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Update failed
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def delete_stock_price_detail_by_stock_detail_id(stock_detail_id, stock_price_time):
    """
    Function to delete a row from the stock_price_detail table using stock_detail_id and stock_price_time.

    Args:
        stock_detail_id (int): The detail ID of the stock.
        stock_price_time (str): The time of the price (in HH:MM:SS format).

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    
    if stock_detail_id is None:
        print(f"Error: No stock_detail found for stock_detail_id {stock_detail_id}.")
        return False  # Cannot delete if no stock_detail_id found
    
    # Proceed to delete the row in stock_price_detail using stock_detail_id and stock_price_time
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # SQL query to delete the row in stock_price_detail based on stock_detail_id and stock_price_time
        query = '''
        DELETE FROM stock_price_detail
        WHERE stock_detail_id = %s AND stock_price_time = %s;
        '''
        
        # Execute the query with the provided parameters
        cur.execute(query, (stock_detail_id, stock_price_time))
        
        # Commit the transaction
        conn.commit()
        
        print(f"Stock price detail deleted successfully for stock_detail_id {stock_detail_id} at {stock_price_time}.")
        return True  # Deletion was successful
    
    except Exception as e:
        print(f"Error deleting stock price detail: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Deletion failed
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def delete_stock_price_detail_by_stock_id_and_date(stock_id, stock_date, stock_price_time):
    """
    Function to delete a row from the stock_price_detail table using stock_id, stock_date, and stock_price_time.
    
    The stock_id and stock_date are used to fetch the stock_detail_id, and the stock_detail_id along
    with the stock_price_time is used to delete the corresponding row.

    Args:
        stock_id (int): The ID of the stock.
        stock_date (int): The stock day to fetch the stock_detail_id.
        stock_price_time (str): The time of the price (in HH:MM:SS format).

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """

    # Import the necessary function that is needed to be called
    from Repository.stock_detail_repository import fetch_stock_detail_id_by_stock_id_and_date
    
    # Fetch the stock_detail_id based on stock_id and stock_date
    stock_detail_id = fetch_stock_detail_id_by_stock_id_and_date(stock_id, stock_date)
    
    if stock_detail_id is None:
        print(f"Error: No stock_detail found for stock_id {stock_id} on day {stock_date}.")
        return False  # Cannot delete if no stock_detail_id found
    
    # Proceed to delete the row in stock_price_detail using stock_detail_id and stock_price_time
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # SQL query to delete the row in stock_price_detail based on stock_detail_id and stock_price_time
        query = '''
        DELETE FROM stock_price_detail
        WHERE stock_detail_id = %s AND stock_price_time = %s;
        '''
        
        # Execute the query with the provided parameters
        cur.execute(query, (stock_detail_id, stock_price_time))
        
        # Commit the transaction
        conn.commit()
        
        print(f"Stock price detail deleted successfully for stock_detail_id {stock_detail_id} at {stock_price_time}.")
        return True  # Deletion was successful
    
    except Exception as e:
        print(f"Error deleting stock price detail: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Deletion failed
    
    finally:
        cur.close()
        connection_pool.putconn(conn)