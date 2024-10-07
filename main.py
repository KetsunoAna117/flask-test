import select
import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_RUNNING

from Domain.stock_news_handler import get_random_news

import os

'''
================================================================================================
Below is Constant
================================================================================================
'''

UPDATE_TIME_SECONDS = 20

'''
================================================================================================
Variables
================================================================================================
'''

app = Flask(__name__)
socketio = SocketIO(app, async_mode="eventlet")



'''
================================================================================================
Business Logic
================================================================================================
'''

def update_stock_prices():
    from Domain.stock_price_handler import change_stock_price 
    from Domain.stock_news_handler import get_random_news
    
    with app.app_context():
        news = get_random_news()
        print("news: ", news)
        change_stock_price(socketio, news)


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
        print("Scheduler from main started.")
    else:
        print("Scheduler from main is already running.")

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
    from Domain.stock_data_client_sender import map_stock_data_to_client

    to_send_stock_data = map_stock_data_to_client()
    return jsonify(to_send_stock_data)

@app.route('/random_news', methods=['GET'])
def random_news():
    news_item = get_random_news()
    return jsonify(news_item)

'''
================================================================================================
Init Function
================================================================================================
'''

if __name__ == '__main__':
    start_scheduler_on_startup()
    port_ws = int(os.environ.get("PORT", 5001))
    socketio.run(app, debug=False, port=port_ws, use_reloader=False)
