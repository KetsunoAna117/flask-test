from Repository.service import connection_pool
from Repository.stock_price_detail import fetch_last_price, fetch_highest_price, fetch_lowest_price

#disini gw ga bikin kalo datanya gak ada di database ya untuk stock detailnya, gw asumsi setiap stock pasti ada stock detail nya nanti pas populate db
def create_new_stock_detail(stock_id):
    """
    Function to create a new stock_detail entry for a specific stock_id.
    
    The stock_day is automatically set to the latest stock_day + 1, 
    and the open_price is set to the last price from the latest stock day.
    The highest_price, lowest_price, and close_price are set to 0.

    Args:
        stock_id (int): The ID of the stock to create a new detail for.

    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    
    # Fetch the last price for the latest stock day using the fetch_last_price_for_latest_day function
    last_price = fetch_last_price_for_latest_day(stock_id)

    # Fetch the latest stock day for the given stock_id
    latest_stock_day = fetch_latest_stock_day(stock_id)

    # new stock day that will be added to the new row of stock_detail table
    new_stock_day = latest_stock_day + 1 
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # Insert the new stock detail with open_price as the last_price, and other prices set to 0
        query = '''
        INSERT INTO stock_detail (stock_id, stock_day, highest_price, lowest_price, open_price, close_price)
        VALUES (%s, %s, %s, %s, %s, %s);
        '''
        
        # Execute the insert query
        cur.execute(query, (stock_id, new_stock_day, 0, 0, last_price, 0))
        
        # Commit the insertion
        conn.commit()
        
        print(f"Stock detail entry created successfully for stock_id {stock_id} on day {new_stock_day}.")
        return True  # Insertion was successful
    
    except Exception as e:
        print(f"Error creating stock detail entry: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Insertion failed
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_stock_detail_info(stock_id):
    """
    Function to fetch detailed information about a stock, including its latest stock detail and price detail.

    Args:
        stock_id: The stock ID to fetch details for.

    Returns:
        dict: A dictionary representing the combined data of stock, stock_detail, and stock_price_detail.
    """
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()

        # Define the SQL query to fetch the required data
        query = '''
        SELECT 
            s.stock_id,
            s.kode_name,
            s.name,
            s.description,
            s.total_shares,
            sd.stock_day,
            sd.highest_price,
            sd.lowest_price,
            sd.open_price,
            sd.close_price,
            spd.price,
            spd.price_time
        FROM stock s
        JOIN stock_detail sd ON s.stock_id = sd.stock_id
        JOIN stock_price_detail spd ON sd.stock_detail_id = spd.stock_detail_id
        WHERE s.stock_id = %s
        ORDER BY sd.stock_day DESC, spd.price_time DESC
        LIMIT 1;
        '''

        # Execute the query with the provided stock_id
        cur.execute(query, (stock_id,))

        # Fetch the result (expecting 1 row)
        result = cur.fetchone()

        # If no result is found, return None or an empty dictionary
        if not result:
            return {}

        # Get column names from the cursor description
        column_names = [desc[0] for desc in cur.description]

        # Convert the result into a dictionary
        stock_detail_info = dict(zip(column_names, result))

        return stock_detail_info

    except Exception as e:
        print(f"Error fetching stock detail info: {e}")
        return {}
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_latest_stock_day(stock_id):
    """
    Function to fetch the latest stock day for a specific stock_id from the stock_detail table.
    
    Args:
        stock_id (int): The ID of the stock to fetch the latest stock day for.
    
    Returns:
        int: The latest stock day, or None if no records are found.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # Query to fetch the latest stock_day for the given stock_id
        query = '''
        SELECT MAX(stock_day)
        FROM stock_detail
        WHERE stock_id = %s;
        '''
        
        # Execute the query to fetch the latest stock day
        cur.execute(query, (stock_id,))
        latest_stock_day_result = cur.fetchone()

        if latest_stock_day_result:
            return latest_stock_day_result[0]  # Return the latest stock day
        else:
            return None  # Return None if no stock day is found
    
    except Exception as e:
        print(f"Error fetching latest stock day for stock_id {stock_id}: {e}")
        return None  # Return None in case of an error
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_last_price_for_latest_day(stock_id):
    """
    Function to fetch the last price for the latest stock day for a specific stock_id.
    
    Args:
        stock_id (int): The ID of the stock to fetch the last price for.
    
    Returns:
        int: The last price for the latest stock day, or 0 if no price or day is found.
    """
    
    # Fetch the latest stock day for the given stock_id
    latest_stock_day = fetch_latest_stock_day(stock_id)
    
    if latest_stock_day is None:
        print(f"No stock day found for stock_id {stock_id}.")
        return 0  # Return 0 if no stock day is found
    
    # Fetch the last price for the latest stock day
    return fetch_last_price(stock_id, latest_stock_day)


def fetch_stock_detail_id(stock_id, stock_day):
    """
    Function to fetch stock_detail_id from stock_detail table based on stock_id and stock_day.

    Args:
        stock_id (int): The ID of the stock.
        stock_day (int): The stock day to retrieve the stock_detail_id.

    Returns:
        int: The stock_detail_id if found, otherwise None.
    """
    
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # SQL query to fetch the stock_detail_id based on stock_id and stock_day
        query = '''
        SELECT stock_detail_id
        FROM stock_detail
        WHERE stock_id = %s AND stock_day = %s;
        '''
        
        # Execute the query
        cur.execute(query, (stock_id, stock_day))
        stock_detail_id_result = cur.fetchone()
        
        if stock_detail_id_result:
            return stock_detail_id_result[0]  # Return stock_detail_id
        else:
            return None  # Return None if not found
    
    except Exception as e:
        print(f"Error fetching stock_detail_id for stock_id {stock_id} on day {stock_day}: {e}")
        return None
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def fetch_stock_details_by_stock_id(stock_id):
    """
    Function to retrieve all stock_day, highest_price, lowest_price, open_price, 
    and close_price from the stock_detail table based on the stock_id.

    Args:
        stock_id (int): The ID of the stock to fetch the details for.

    Returns:
        list: A list of dictionaries representing the stock_day, highest_price, 
              lowest_price, open_price, and close_price for the given stock_id.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # SQL query to fetch stock details based on stock_id
        query = '''
        SELECT stock_day, highest_price, lowest_price, open_price, close_price
        FROM stock_detail
        WHERE stock_id = %s;
        '''
        
        # Execute the query
        cur.execute(query, (stock_id,))
        stock_details_data = cur.fetchall()
        
        # Get column names from the cursor
        column_names = [desc[0] for desc in cur.description]
        
        # Convert the result into a list of dictionaries
        stock_details_result = [dict(zip(column_names, row)) for row in stock_details_data]
        
        return stock_details_result
    
    except Exception as e:
        print(f"Error fetching stock details for stock_id {stock_id}: {e}")
        return []
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def update_stock_detail_from_db(stock_id, stock_day):
    """
    Function to update the highest_price, lowest_price, and close_price in stock_detail 
    for a given stock_id and stock_day, based on stock_price_detail.

    Args:
        stock_id (int): The ID of the stock to update.
        stock_day (int): The stock day to update the details for.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # Fetch the highest price for the stock_id and stock_day
        highest_price = fetch_highest_price(stock_id, stock_day)
        
        # Fetch the lowest price for the stock_id and stock_day
        lowest_price = fetch_lowest_price(stock_id, stock_day)
        
        # Fetch the close price using the last price of the day
        close_price = fetch_last_price(stock_id, stock_day)

        # Update the stock_detail table with the new highest, lowest, and close prices
        query = '''
        UPDATE stock_detail
        SET highest_price = %s, lowest_price = %s, close_price = %s
        WHERE stock_id = %s AND stock_day = %s;
        '''
        
        cur.execute(query, (highest_price, lowest_price, close_price, stock_id, stock_day))
        
        # Commit the transaction
        conn.commit()
        
        print(f"Stock detail for stock_id {stock_id} on day {stock_day} updated successfully.")
        return True
    
    except Exception as e:
        print(f"Error updating stock detail for stock_id {stock_id} on day {stock_day}: {e}")
        conn.rollback()  # Rollback in case of error
        return False
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def update_stock_detail_with_values(stock_id, stock_day, highest_price, lowest_price, close_price):
    """
    Function to update the highest_price, lowest_price, and close_price in stock_detail 
    for a given stock_id and stock_day, with explicitly provided values.

    Args:
        stock_id (int): The ID of the stock to update.
        stock_day (int): The stock day to update the details for.
        highest_price (int): The updated highest price.
        lowest_price (int): The updated lowest price.
        close_price (int): The updated close price.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    
    conn = connection_pool.getconn()
    
    try:
        cur = conn.cursor()

        # Update the stock_detail table with the provided highest, lowest, and close prices
        query = '''
        UPDATE stock_detail
        SET highest_price = %s, lowest_price = %s, close_price = %s
        WHERE stock_id = %s AND stock_day = %s;
        '''
        
        # Execute the query with the given values
        cur.execute(query, (highest_price, lowest_price, close_price, stock_id, stock_day))
        
        # Commit the transaction
        conn.commit()
        
        print(f"Stock detail for stock_id {stock_id} on day {stock_day} updated successfully.")
        return True
    
    except Exception as e:
        print(f"Error updating stock detail for stock_id {stock_id} on day {stock_day}: {e}")
        conn.rollback()  # Rollback in case of error
        return False
    
    finally:
        cur.close()
        connection_pool.putconn(conn)


def delete_stock_detail_from_db(stock_id, stock_day):
    """
    Function to delete a row from the stock_detail table based on stock_id and stock_day.
    
    Args:
        stock_id (int): The ID of the stock to delete.
        stock_day (int): The stock day to delete.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # SQL query to delete the row from stock_detail based on stock_id and stock_day
        query = '''
        DELETE FROM stock_detail
        WHERE stock_id = %s AND stock_day = %s;
        '''
        
        # Execute the delete query
        cur.execute(query, (stock_id, stock_day))
        
        # Commit the transaction
        conn.commit()
        
        print(f"Stock detail with stock_id {stock_id} and stock_day {stock_day} deleted successfully.")
        return True
    
    except Exception as e:
        print(f"Error deleting stock detail for stock_id {stock_id} on day {stock_day}: {e}")
        conn.rollback()  # Rollback in case of an error
        return False
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)
