from dotenv import load_dotenv
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING

from Repository.stock_repository import fetch_stock_by_stock_id
from Repository.stock_price_detail import fetch_last_price, create_stock_price_detail

from Domain.server_date_handler import get_current_date, get_current_time
from Domain.stock_data_client_sender import map_stock_data_to_client

from Model.Stock import StockDTO

# Constants
SECONDS_START_RUNNING = 0  # The seconds to start running the scheduler
UPDATE_STOCK_PRICE_SECONDS = 2 # The interval to update the stock price in seconds
TOTAL_PRICE_CHANGE_SECONDS = 10  # The total time for the price change to happen in seconds

FIRST_INDEX = 0  # The first index in a list

def change_stock_price(socketIO, news):
    """
    Function to change stock price based on the news data.

    Args:
       socketIO: The SocketIO instance to emit the updated stock price.
       news: The news data to get the stock_id and percentage change.
    
    Returns:
        NONE
    """
        
    if news is None:
        print("No news data available.")
        return None
    
    stock_id = news.get('stock_id')  # Get the stock_id from the news data
    
    current_price = fetch_last_price(stock_id, get_current_date()) # Get the current stock price from the query result
    percentage_change = news.get('news_value_fluctuation')  # Get the percentage change from news tuple (assumed to be a float representing a percentage)
    target_price = int(current_price + (current_price * (percentage_change / 100))) # Calculate the target price based on the percentage change
    
    price_difference = target_price - current_price # Calculate the price difference
    times_change = TOTAL_PRICE_CHANGE_SECONDS / UPDATE_STOCK_PRICE_SECONDS # Calculate how many times the price will change
    price_change_per_second = int(price_difference / times_change) # Calculate the price change per second

    print(f"Current price: {current_price}, Percentage Change: {percentage_change}, Target price: {target_price}, Price change per second: {price_change_per_second}")

    # Start the scheduler to update the stock price towards the target
    handle_scheduler(stock_id, current_price, socketIO, price_change_per_second)

def handle_scheduler(stock_id: int, current_price: int, socketIO, price_change_per_second: int):
    current_stock_price = {
        "current_price": current_price
    }  # Get the current stock price

    scheduler = BackgroundScheduler()

    # Add the job to change stock price every 1 second, passing arguments
    scheduler.add_job(func=handle_price_change, args=[stock_id, socketIO, current_stock_price, price_change_per_second, scheduler], 
                    trigger="interval", seconds=UPDATE_STOCK_PRICE_SECONDS, 
                    id="price_change_job", max_instances=1)
        
    
    start_scheduler(scheduler)


def handle_price_change(stock_id: int, socketIO, current_stock_price: dict, price_change_per_second, scheduler):
    # Track how many seconds have passed
    if not hasattr(handle_price_change, 'seconds_passed'):
        handle_price_change.seconds_passed = SECONDS_START_RUNNING  # Initialize if not already set

    # Stop updating after the total duration (10 seconds)
    if handle_price_change.seconds_passed >= TOTAL_PRICE_CHANGE_SECONDS:
        print(f"Price change completed after {TOTAL_PRICE_CHANGE_SECONDS} seconds with final result: {current_stock_price}")
        handle_price_change.seconds_passed = 0  # Reset for next use
        stop_scheduler(scheduler)  # Stop the scheduler
        return  # Exit the function after the total time

    # Calculate the new stock price
    current_stock_price['current_price'] += price_change_per_second

    # TODO: Uncomment this function below to push the updated stock price in the database
    # create_stock_price_detail(stock_id, get_current_date(), current_stock_price['current_price'], get_current_time())

    to_send_stock = map_stock_data_to_client()  # Get the updated stock data to send to the client
    socketIO.emit('update_stock', to_send_stock)  # Emit the updated stock data
        
    print(f"Stock price updated to: {current_stock_price} at {handle_price_change.seconds_passed + UPDATE_STOCK_PRICE_SECONDS} seconds.")

    # Increment the seconds passed
    handle_price_change.seconds_passed += UPDATE_STOCK_PRICE_SECONDS


 
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

