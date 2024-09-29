from flask import Flask, jsonify
from Model.Person import Person

import os

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({"Choo Choo": "Welcome to your Flask app 🚅"})

@app.route('/person', methods=['GET'])
def get_person():
    person = Person("John", 30)
    return jsonify(person.to_dict())

if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))
