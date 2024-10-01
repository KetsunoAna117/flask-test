import select
import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
from Model.Stock import Stock
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING
import random
import os
import mysql.connector  # Import MySQL connector
from mysql.connector import Error  # Import the Error exception class

UPDATE_TIME_SECONDS = 5

app = Flask(__name__)
socketio = SocketIO(app, async_mode="eventlet")

# MySQL database connection parameters
DB_CONFIG = {
    'host': 'localhost',  # Host where MySQL is running
    'port': 3306,         # MySQL port, default is 3306
    'database': 'hanvest',  # Your database name
}

# Function to get MySQL database connection
def get_db_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        app.logger.error(f"Error connecting to MySQL: {e}")
        return None

# Function to retrieve stock data from the database
def get_stock_data_from_db():
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT s.KodeName, s.Name, sp.Price 
        FROM Stock s
        JOIN StockDetail sd ON s.StockDetailID = sd.StockDetailID
        JOIN StockPriceDetail sp ON sd.StockPriceDetailID = sp.StockPriceDetailID
        """
        cursor.execute(query)
        stocks = cursor.fetchall()
        cursor.close()
        connection.close()
        return stocks
    return []

# Example Stock data
stock_data = []

# Function to simulate stock price changes
# def update_stock_prices():
#     index_stock_to_change = random.randint(0, len(stock_data) - 1)
#     stock_data[index_stock_to_change].price += random.choice([-1, 1]) * random.randint(1, 3)

#     stock_list = [stock.to_dict() for stock in stock_data]
#     app.logger.info("Stock prices updated: %s", stock_list)

#     # Ensure emit happens in Flask-SocketIO context
#     with app.app_context():
#         socketio.emit('update_stock', stock_list)
#         socketio.emit('new_stock_event', "New Stock Event Triggered")

# Function to simulate stock price changes
def update_stock_prices():
    stock_list = get_stock_data_from_db()
    app.logger.info("Stock data: %s", stock_list)

    # Emit the updated stock list to all connected clients
    with app.app_context():
        socketio.emit('update_stock', stock_list)
        socketio.emit('new_stock_event', "New Stock Event Triggered")

# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(func=update_stock_prices, trigger="interval", seconds=UPDATE_TIME_SECONDS)

def start_scheduler_on_startup():
    if scheduler.state != STATE_RUNNING:
        scheduler.start()
        print("Scheduler started.")
    else:
        print("Scheduler already running.")

@socketio.on('connect')
def handle_connect():
    print("Client connected")

    # Optional: Add a small delay to ensure the client is ready to receive the event
    socketio.sleep(1)  # Sleep for 1 second

    stock_list = [stock.to_dict() for stock in stock_data]
    socketio.emit('update_stock', stock_list)

@socketio.on('disconnect')
def handle_disconnect():
    print("Client disconnected")

@socketio.on('user_input')
def handle_user_input(data):
    with app.app_context():
        socketio.emit('user_input_response', "Received user input: " + data)
    print("User input received: ", data)

@app.route('/stock', methods=['GET'])
def get_stock():
    stock_list = [stock.to_dict() for stock in stock_data]
    return jsonify(stock_list)

if __name__ == '__main__':
    start_scheduler_on_startup()
    port_ws = int(os.environ.get("PORT", 5001))
    socketio.run(app, debug=True, port=port_ws, use_reloader=False)
