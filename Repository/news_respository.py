from Repository.service import connection_pool

def create_news_in_db(stock_id, news_description, news_val_fluks):
    """
    Function to create a new news entry in the 'news' table.

    Args:
        stock_id (int): The ID of the stock related to the news.
        news_description (str): The description of the news.
        news_val_fluks (int): The value fluctuation associated with the news.
    
    Returns:
        bool: True if the insertion was successful, False otherwise.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()
        
        # SQL query to insert a new row into the news table
        query = '''
        INSERT INTO news (stock_id, news_description, news_val_fluks)
        VALUES (%s, %s, %s);
        '''
        
        # Execute the insert query with the provided values
        cur.execute(query, (stock_id, news_description, news_val_fluks))
        
        # Commit the insertion
        conn.commit()
        
        print(f"News entry created successfully for stock_id {stock_id}.")
        return True  # Insertion was successful
    
    except Exception as e:
        print(f"Error creating news entry: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Insertion failed
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def fetch_news_from_db():
    """
    Function to retrieve all news from the 'news' table.

    Returns:
        list: A list of dictionaries representing each news item.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()

        # SQL query to fetch all rows from the news table
        query = '''
        SELECT * FROM news;
        '''
        
        # Execute the query to fetch all rows from the news table
        cur.execute(query)
        news_data = cur.fetchall()
        
        # Get column names from the cursor
        column_names = [desc[0] for desc in cur.description]
        
        # Convert the result into a list of dictionaries
        news_result = [dict(zip(column_names, row)) for row in news_data]
        
        return news_result
    
    except Exception as e:
        print(f"Error fetching news from database: {e}")
        return []
    
    finally:
        # Always ensure that the connection is returned to the pool
        cur.close()
        connection_pool.putconn(conn)


def update_news_in_db(news_id, stock_id, news_description, news_val_fluks):
    """
    Function to update a news entry in the 'news' table based on the news_id.

    Args:
        news_id (int): The ID of the news item to update.
        stock_id (int): The stock ID associated with the news.
        news_description (str): The updated news description.
        news_val_fluks (int): The updated news value fluctuation.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()
        
        # SQL query to update the news entry
        query = '''
        UPDATE news
        SET stock_id = %s, news_description = %s, news_val_fluks = %s
        WHERE news_id = %s;
        '''
        
        # Execute the update query with the given parameters
        cur.execute(query, (stock_id, news_description, news_val_fluks, news_id))
        
        # Commit the changes
        conn.commit()
        
        print(f"News entry with id {news_id} updated successfully.")
        return True  # Update was successful
    
    except Exception as e:
        print(f"Error updating news entry with id {news_id}: {e}")
        conn.rollback()  # Rollback in case of error
        return False  # Update failed
    
    finally:
        # close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)


def delete_news_from_db(news_id):
    """
    Function to delete a news entry from the 'news' table based on the news_id.

    Args:
        news_id (int): The ID of the news item to delete.
    
    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()
        
        # SQL query to delete the news entry
        query = '''
        DELETE FROM news WHERE news_id = %s;
        '''
        
        # Execute the delete query with the given news_id
        cur.execute(query, (news_id,))
        
        # Commit the deletion
        conn.commit()
        
        print(f"News entry with id {news_id} deleted successfully.")
        return True  # Deletion was successful
    
    except Exception as e:
        print(f"Error deleting news entry with id {news_id}: {e}")
        conn.rollback()  # Rollback in case of an error
        return False  # Deletion failed
    
    finally:
        # Close the cursor and return the connection to the pool
        cur.close()
        connection_pool.putconn(conn)

