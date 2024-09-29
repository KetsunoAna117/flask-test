from flask import Flask, jsonify
from Model.Person import Person

import os

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({"Choo Choo": "Welcome to your Flask app 🚅"})

@app.route('/person', methods=['GET'])
def get_person():
    person1 = Person("John", 30).to_dict()
    person2 = Person("Jane", 25).to_dict()
    
    return jsonify([person1, person2])

if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))
