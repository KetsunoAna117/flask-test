from flask import Flask, jsonify
from Model.Stock import Stock
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING
import random

import os

UPDATE_TIME_MINUTES = 10

app = Flask(__name__)

# Example Stock data
stock_data = [
    Stock(name="GOTO", price=50),
    Stock(name="BBCA", price=10000),
    Stock(name="BBRI", price=2000),
]

# Function to simulate stock price changes
def update_stock_prices():
    index_stock_to_change = random.randint(0, len(stock_data) - 1)

    # Randomly increase or decrease the stock price
    stock_data[index_stock_to_change].price += random.choice([-1, 1]) * random.randint(1, 3)

# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(func=update_stock_prices, trigger="interval", minutes=UPDATE_TIME_MINUTES)

@app.before_first_request
def start_scheduler_on_startup():
    if scheduler.state != STATE_RUNNING:
        scheduler.start()
        print("Scheduler started.")

# @app.teardown_appcontext
# def shutdown_scheduler_on_teardown(exception=None):
#     if scheduler.state == STATE_RUNNING:
#         scheduler.shutdown()
#         print("Scheduler shut down.")

def start_scheduler():
    if scheduler.state != STATE_RUNNING:
        scheduler.start()
        print("Scheduler started.")

@app.route('/')
def index():
    return jsonify({"Choo Choo": "Welcome to your Flask app 🚅"})

@app.route('/stock', methods=['GET'])
def get_stock():
    stocks_to_send = []

    for stock in stock_data:
        stocks_to_send.append(stock.to_dict())

    return jsonify(stocks_to_send)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, use_reloader=False, port=port)


