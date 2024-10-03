import random

def get_random_news(connection_pool):
    """
    Function to retrieve a random news entry from the 'news' table.
    
    Returns:
        tuple: A Tuple representing a random news item.
    """
    
    # Get a connection from the pool
    conn = connection_pool.getconn()
    
    try:
        # create a cursor 
        cur = conn.cursor()
        
        # Query to fetch all rows from the news table
        cur.execute('''SELECT * FROM news''')
        news_data = cur.fetchall()
        
        # If there is data in the news table, choose a random entry
        if news_data:
            # Get column names from the cursor
            column_names = [desc[0] for desc in cur.description]
            
            # Choose a random news item from the list
            random_news = random.choice(news_data)
            return random_news
            
            # # Convert the selected news into a dictionary
            # random_news_dict = dict(zip(column_names, random_news))
        else:
            # random_news_dict = {}
            return None
    
    except Exception as e:
        print(f"Error fetching random news: {e}")
        return {}
    
    finally:
        # Always ensure that the connection is returned to the pool
        connection_pool.putconn(conn)

