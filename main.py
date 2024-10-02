import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING
from dotenv import load_dotenv
import os
import psycopg2

'''
================================================================================================
Below is Constant
================================================================================================
'''

UPDATE_TIME_SECONDS = 5

'''
================================================================================================
Variables
================================================================================================
'''

app = Flask(__name__)
socketio = SocketIO(app, async_mode="eventlet")

# Load environtment variables
load_dotenv()

# Initialize a connection pool globally (size 1-10 connections)
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 10, 
    user=os.getenv("DATABASE_USERNAME"), 
    password=os.getenv("DATABASE_PASSWORD"), 
    host=os.getenv("localhost"), 
    port=os.getenv("DATABASE_PORT"), 
    database=os.getenv("DATABASE_NAME")
)

'''
================================================================================================
Business Logic
================================================================================================
'''

def update_stock_prices():
    # Get a connection from the pool
    conn = connection_pool.getconn()
  
    # create a cursor 
    cur = conn.cursor() 

    cur.execute('''SELECT * FROM stock''')
    data = cur.fetchall()

    # close connection
    cur.close()
    connection_pool.putconn(conn)  # return to the pool

     # Get column names from the cursor
    column_names = [desc[0] for desc in cur.description]

    # Convert the result into a list of dictionaries
    stock_result = [dict(zip(column_names, row)) for row in data]

    # Ensure emit happens in Flask-SocketIO context
    with app.app_context():
        socketio.emit('update_stock', stock_result) # Emit the updated stock data
        socketio.emit('new_stock_event', "New Stock Event Triggered")


'''
================================================================================================
Scheduler Function
================================================================================================
'''

# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(func=update_stock_prices, trigger="interval", seconds=UPDATE_TIME_SECONDS)

def start_scheduler_on_startup():
    if scheduler.state != STATE_RUNNING:
        scheduler.start()
        print("Scheduler started.")
    else:
        print("Scheduler already running.")

'''
================================================================================================
Below is SocketIO function
================================================================================================
'''

@socketio.on('connect')
def handle_connect():
    print("Client connected")

@socketio.on('disconnect')
def handle_disconnect():
    print("Client disconnected")

@socketio.on('user_input')
def handle_user_input(data):
    with app.app_context():
        socketio.emit('user_input_response', "Received user input: " + data)
    print("User input received: ", data)


'''
================================================================================================
Below is HTTP function
================================================================================================
'''

@app.route('/stock', methods=['GET'])
def get_stock():
     # Get a connection from the pool
    conn = connection_pool.getconn()
  
    # create a cursor 
    cur = conn.cursor() 

    cur.execute('''SELECT * FROM stock''')
    data = cur.fetchall()

    # close connection
    cur.close()
    connection_pool.putconn(conn)  # return to the pool

    # Get column names from the cursor
    column_names = [desc[0] for desc in cur.description]

    # Convert the result into a list of dictionaries
    stock_result = [dict(zip(column_names, row)) for row in data]

    return jsonify(stock_result)
'''
================================================================================================
Init Function
================================================================================================
'''

if __name__ == '__main__':
    start_scheduler_on_startup()
    port_ws = int(os.environ.get("PORT", 5001))
    socketio.run(app, debug=True, port=port_ws, use_reloader=False)
