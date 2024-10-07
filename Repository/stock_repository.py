from Repository.service import connection_pool

def create_stock_in_db(code_name, stock_name, stock_description, stock_total_shares):
    """
    Function to create a new stock entry in the 'stock' table.

    Args:
        code_name (str): The stock code (e.g., 'AAPL' for Apple).
        stock_name (str): The full name of the stock (e.g., 'Apple Inc.').
        stock_description (str): A description of the stock.
        stock_total_shares (int): The total number of shares available.
    
    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()
        
        # SQL query to insert a new stock into the stock table
        query = '''
        INSERT INTO stock (code_name, stock_name, stock_description, stock_total_shares)
        VALUES (%s, %s, %s, %s);
        '''
        
        # Execute the insert query with the provided values
        cur.execute(query, (code_name, stock_name, stock_description, stock_total_shares))
        
        # Commit the insertion
        conn.commit()
        
        print(f"Stock entry created successfully for {stock_name} ({code_name}).")
        return True  # Insertion was successful
    
    except Exception as e:
        print(f"Error creating stock entry: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Insertion failed
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_stock_by_stock_id(stock_id):
    """
    Function to retrieve a specific stock by stock_id from the 'stock' table.

    Args:
        stock_id (int): The ID of the stock to fetch.

    Returns:
        dict: A dictionary representing the stock item if found, otherwise None.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # SQL query to fetch a stock by its ID
        query = '''
        SELECT * FROM stock
        WHERE stock_id = %s;
        '''
        
        # Execute the query to fetch the specific stock item
        cur.execute(query, (stock_id,))
        stock_data = cur.fetchone()
        
        # If no stock is found, return None
        if not stock_data:
            print(f"No stock found with stock_id {stock_id}")
            return None
        
        # Get column names from the cursor
        column_names = [desc[0] for desc in cur.description]
        
        # Convert the result into a dictionary
        stock_result = dict(zip(column_names, stock_data))
        
        return stock_result
    
    except Exception as e:
        print(f"Error fetching stock with stock_id {stock_id} from database: {e}")
        return None
    
    finally:
        # Ensure that the cursor is closed and the connection is returned to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_all_stock_from_db():
    """
    Function to retrieve all stocks from the database.
    
    Returns:
        list: A list of dictionaries representing all stocks.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()
        
        # Execute the query to fetch all rows from the stock table
        cur.execute('''SELECT * FROM stock''')
        data = cur.fetchall()
        
        # Get column names from the cursor
        column_names = [desc[0] for desc in cur.description]
        
        # Convert the result into a list of dictionaries
        stock_result = [dict(zip(column_names, row)) for row in data]
        
        return stock_result
    
    except Exception as e:
        print(f"Error fetching stock from database: {e}")
        return []
    
    finally:
        # Ensure that the cursor is closed and the connection is returned to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_stock_id_by_code_name(code_name):
    """
    Function to retrieve the stock_id based on the code_name.

    Args:
        code_name (str): The stock's code name (e.g., 'AAPL').

    Returns:
        int: The stock_id if found, otherwise None.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # SQL query to fetch the stock_id based on code_name
        query = '''
        SELECT stock_id
        FROM stock
        WHERE code_name = %s;
        '''
        
        # Execute the query
        cur.execute(query, (code_name,))
        stock_id_result = cur.fetchone()
        
        # If stock_id is found, return it, otherwise return None
        return stock_id_result[0] if stock_id_result else None
    
    except Exception as e:
        print(f"Error fetching stock_id for code_name {code_name}: {e}")
        return None
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def update_stock_in_db_by_stock_id(stock_id, code_name, stock_name, stock_description, stock_total_shares):
    """
    Function to update a stock entry in the 'stock' table based on the stock_id.

    Args:
        stock_id (int): The ID of the stock to update.
        code_name (str): The updated stock code (e.g., 'AAPL' for Apple).
        stock_name (str): The updated name of the stock (e.g., 'Apple Inc.').
        stock_description (str): The updated description of the stock.
        stock_total_shares (int): The updated total number of shares.
    
    Returns:
        bool: True if the update was successful, False otherwise.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()
        
        # SQL query to update the stock entry
        query = '''
        UPDATE stock
        SET code_name = %s, stock_name = %s, stock_description = %s, stock_total_shares = %s
        WHERE stock_id = %s;
        '''
        
        # Execute the update query with the provided values
        cur.execute(query, (code_name, stock_name, stock_description, stock_total_shares, stock_id))
        
        # Commit the update
        conn.commit()
        
        print(f"Stock entry with id {stock_id} updated successfully.")
        return True  # Update was successful
    
    except Exception as e:
        print(f"Error updating stock entry with id {stock_id}: {e}")
        conn.rollback()  # Rollback in case of an error
        return False  # Update failed
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def delete_stock_from_db_by_stock_id(stock_id):
    """
    Function to delete a stock entry from the 'stock' table based on the stock_id.

    Args:
        stock_id (int): The ID of the stock to delete.
    
    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()
        
        # SQL query to delete the stock entry
        query = '''
        DELETE FROM stock WHERE stock_id = %s;
        '''
        
        # Execute the delete query with the provided stock_id
        cur.execute(query, (stock_id,))
        
        # Commit the deletion
        conn.commit()
        
        print(f"Stock entry with id {stock_id} deleted successfully.")
        return True  # Deletion was successful
    
    except Exception as e:
        print(f"Error deleting stock entry with id {stock_id}: {e}")
        conn.rollback()  # Rollback in case of an error
        return False  # Deletion failed
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)
