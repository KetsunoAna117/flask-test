import random
from Repository.news_respository import fetch_news_from_db

def get_random_news():
    """
    Function to retrieve a random news entry from the 'news' table.
    
    Returns:
        dict: A dictionary representing a random news item.
    """
    
    # Fetch all news from the database
    news_data = fetch_news_from_db()
    
    # If there is data in the news table, choose a random entry
    if news_data:
        random_news = random.choice(news_data)
    else:
        random_news = {}
        
    return random_news

