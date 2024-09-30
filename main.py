from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
from Model.Stock import Stock
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING
from flask_cors import CORS
import random

import os

UPDATE_TIME_MINUTES = 1
UPDATE_TIME_SECONDS = 10

app = Flask(__name__)
socketio = SocketIO(app)

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

    # Send updated stock prices to all connected clients via WebSocket
    stock_list = [stock.to_dict() for stock in stock_data]
    app.logger.info("Stock prices updated: %s", stock_list)
    socketio.emit('update_stock', stock_list)
    socketio.emit('new_stock_event', "New Stock Event Triggered")

# Initialize the scheduler
scheduler = BackgroundScheduler()
# scheduler.add_job(func=update_stock_prices, trigger="interval", minutes=UPDATE_TIME_MINUTES)
scheduler.add_job(func=update_stock_prices, trigger="interval", seconds=UPDATE_TIME_SECONDS)

# @app.before_first_request
def start_scheduler_on_startup():
    if scheduler.state != STATE_RUNNING:
        scheduler.start()
        print("Scheduler started.")
    else:
        print("Scheduler already running.")

@socketio.on('connect')
def handle_connect():
    print("Client connected")

@socketio.on('disconnect')
def handle_disconnect():
    print("Client disconnected")

@socketio.on('user_input')
def handle_user_input(data):
    socketio.emit('user_input_response', "Received user input: " + data)
    print("User input received: ", data)

# @app.teardown_appcontext
# def shutdown_scheduler_on_teardown(exception=None):
#     if scheduler.state == STATE_RUNNING:
#         scheduler.shutdown()
#         print("Scheduler shut down.")

# def start_scheduler():
#     if scheduler.state != STATE_RUNNING:
#         scheduler.start()
#         print("Scheduler started.")

# @app.route('/')
# def index():
#     return jsonify({"Choo Choo": "Welcome to your Flask app 🚅"})

# @app.route('/stock', methods=['GET'])
def get_stock():
    stocks_to_send = [stock.to_dict() for stock in stock_data]
    return jsonify(stocks_to_send)

# if __name__ == '__main__':
#     port = int(os.environ.get("PORT", 5000))
#     app.run(debug=True, use_reloader=False, port=port)

if __name__ == '__main__':
    start_scheduler_on_startup()
    port = int(os.environ.get("PORT", 5001))
    socketio.run(app, debug=True, port=port, use_reloader=False, host="0.0.0.0")



