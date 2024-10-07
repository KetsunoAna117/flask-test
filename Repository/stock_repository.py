from Repository.service import connection_pool

def create_stock_in_db(kode_name, name, description, total_shares):
    """
    Function to create a new stock entry in the 'stock' table.

    Args:
        kode_name (str): The stock code (e.g., 'AAPL' for Apple).
        name (str): The full name of the stock (e.g., 'Apple Inc.').
        description (str): A description of the stock.
        total_shares (int): The total number of shares available.
    
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
        INSERT INTO stock (kode_name, name, description, total_shares)
        VALUES (%s, %s, %s, %s);
        '''
        
        # Execute the insert query with the provided values
        cur.execute(query, (kode_name, name, description, total_shares))
        
        # Commit the insertion
        conn.commit()
        
        print(f"Stock entry created successfully for {name} ({kode_name}).")
        return True  # Insertion was successful
    
    except Exception as e:
        print(f"Error creating stock entry: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Insertion failed
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_stock_from_db():
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


def fetch_stock_id_by_kode_name(kode_name):
    """
    Function to retrieve the stock_id based on the kode_name.

    Args:
        kode_name (str): The stock's code name (e.g., 'AAPL').

    Returns:
        int: The stock_id if found, otherwise None.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # Create a cursor
        cur = conn.cursor()

        # SQL query to fetch the stock_id based on kode_name
        query = '''
        SELECT stock_id
        FROM stock
        WHERE kode_name = %s;
        '''
        
        # Execute the query
        cur.execute(query, (kode_name,))
        stock_id_result = cur.fetchone()
        
        # If stock_id is found, return it, otherwise return None
        return stock_id_result[0] if stock_id_result else None
    
    except Exception as e:
        print(f"Error fetching stock_id for kode_name {kode_name}: {e}")
        return None
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def update_stock_in_db(stock_id, kode_name, name, description, total_shares):
    """
    Function to update a stock entry in the 'stock' table based on the stock_id.

    Args:
        stock_id (int): The ID of the stock to update.
        kode_name (str): The updated stock code (e.g., 'AAPL' for Apple).
        name (str): The updated name of the stock (e.g., 'Apple Inc.').
        description (str): The updated description of the stock.
        total_shares (int): The updated total number of shares.
    
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
        SET kode_name = %s, name = %s, description = %s, total_shares = %s
        WHERE stock_id = %s;
        '''
        
        # Execute the update query with the provided values
        cur.execute(query, (kode_name, name, description, total_shares, stock_id))
        
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


def delete_stock_from_db(stock_id):
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
