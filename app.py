from flask import Flask, request
from flask_cors import CORS
from time_series_functions import index
import json


app = Flask(__name__)
CORS(app)

# def thing():

@app.route("/stockDetails/<stock_abv>/<datapoint>", methods=['GET'])
def hello_world(stock_abv, datapoint='close'):
    index.get_data()
    volume = index.get_datapoints(datapoint)
    datetime = index.get_datetime()
    volume.reverse()
    datetime.reverse()
    print("working")
    return {
        "volume": volume,
        "datetime": datetime
    }

@app.route("/something/<id>", methods=['POST', 'GET'])
def something(id):
    return {"response": id}
