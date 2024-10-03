from dotenv import load_dotenv
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING
import time

# Constants
UPDATE_STOCK_PRICE_SECONDS = 1 # The interval to update the stock price in seconds
SECONDS_START_RUNNING = 0  # The seconds to start running the scheduler
TOTAL_PRICE_CHANGE_SECONDS = 5  # The total time for the price change to happen in seconds

STOCK_PRICE_INDEX = 5  # The index of the stock price in the query result
NEWS_PRICE_CHANGE_INDEX = 3  # The index of the price change in the news data
FIRST_INDEX = 0  # The first index in a list

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

    return data[FIRST_INDEX]

# This function will change the stock price periodically based on the news
def change_stock_price(connection_pool, socketIO, news):
    if news is None:
        print("No news data available.")
        return None
    
     # Get a connection from the pool
    conn = connection_pool.getconn()
  
    # create a cursor 
    cur = conn.cursor() 

    cur.execute('''SELECT * FROM stock WHERE stock_id = %s''', (news[1],)) # psycopg2 only accepts tuples as parameters
    stock = cur.fetchall()

    # Check if stock exists
    if not stock:
        print(f"No stock found for stock_id {news[1]}")
        cur.close()
        connection_pool.putconn(conn)
        return None
    
    current_price = stock[FIRST_INDEX][STOCK_PRICE_INDEX] # Get the current stock price from the query result
    percentage_change = news[NEWS_PRICE_CHANGE_INDEX]  # Get the percentage change from news[3] (assumed to be a float representing a percentage)
    target_price = int(current_price + (current_price * (percentage_change / 100))) # Calculate the target price based on the percentage change
    
    price_difference = target_price - current_price # Calculate the price difference
    price_change_per_second = int(price_difference / TOTAL_PRICE_CHANGE_SECONDS) # Calculate the price change per second

    print(f"Current price: {current_price}, Target price: {target_price}, Price change per second: {price_change_per_second}")

    # Start the scheduler to update the stock price towards the target
    handle_scheduler(connection_pool, stock, socketIO, price_change_per_second)

    # close connection
    cur.close()
    connection_pool.putconn(conn)  # return to the pool

    return stock

def handle_scheduler(connection_pool, stock, socketIO, price_change_per_second):
    stock_id = stock[FIRST_INDEX][FIRST_INDEX]  # Assuming the stock_id is at the first index
    current_stock_price = [stock[FIRST_INDEX][STOCK_PRICE_INDEX]]  # Get the current stock price

    scheduler = BackgroundScheduler()

    # Add the job to change stock price every 1 second, passing arguments
    scheduler.add_job(func=handle_price_change, args=[connection_pool, stock_id, current_stock_price, price_change_per_second, scheduler], 
                    trigger="interval", seconds=UPDATE_STOCK_PRICE_SECONDS, 
                    id="price_change_job", max_instances=1)
        
    
    start_scheduler(scheduler)


def handle_price_change(connection_pool, stock_id, current_stock_price, price_change_per_second, scheduler):
    # Track how many seconds have passed
    if not hasattr(handle_price_change, 'seconds_passed'):
        handle_price_change.seconds_passed = SECONDS_START_RUNNING  # Initialize if not already set

    # Stop updating after the total duration (10 seconds)
    if handle_price_change.seconds_passed >= TOTAL_PRICE_CHANGE_SECONDS:
        print(f"Price change completed after {TOTAL_PRICE_CHANGE_SECONDS} seconds.")
        handle_price_change.seconds_passed = 0  # Reset for next use
        stop_scheduler(scheduler)  # Stop the scheduler
        return  # Exit the function after the total time

    # Calculate the new stock price
    current_stock_price[FIRST_INDEX] += price_change_per_second

    # Update the stock price in the database
    conn = connection_pool.getconn()
    cur = conn.cursor()

    cur.execute('''UPDATE stock SET total_shares = %s WHERE stock_id = %s''', (current_stock_price[FIRST_INDEX], stock_id))

    # Commit the changes
    conn.commit()

    # close connection
    cur.close()
    connection_pool.putconn(conn)  # return to the pool

    # Emit the new stock price to the client
    # socketIO.emit("stock_price_change", {"stock_id": stock_id, "stock_price": current_stock_price})

    print(f"Stock price updated to: {current_stock_price} at {handle_price_change.seconds_passed} seconds.")

    # Increment the seconds passed
    handle_price_change.seconds_passed += 1


 
def start_scheduler(scheduler):
    if scheduler.state != STATE_RUNNING:
        scheduler.start()
        print("Scheduler for price changing started.")
    else:
        print("Scheduler for price changing is already running.")

def stop_scheduler(scheduler):
    if scheduler.state == STATE_RUNNING:
        scheduler.shutdown(wait=False)  # Don't wait for running jobs to finish
        print("Scheduler for price changing stopped.")
    else:
        print("Scheduler for price changing has been already stopped.")

