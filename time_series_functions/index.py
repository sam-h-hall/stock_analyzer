import pandas as pd
import numpy as np
import math
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import json

def get_data():
    series_data = open("time_series_functions/stock_data.json")
    train_data = pd.read_json(series_data)
    print("DATA: \n", train_data.describe())

# datapoint: 'volume', 'close', 'high', 'low', 'open'
def get_datapoints(datapoint):
    series_data = open("time_series_functions/stock_data.json")
    volume = pd.read_json(series_data)[datapoint].values
    return volume.tolist()
    
def get_datetime():
    # for now we are reading files, later will be getting from server
    series_data = open("time_series_functions/stock_data.json")
    datetime = pd.read_json(series_data)['datetime'].values
    new_date_list = [];
    for idx, date in enumerate(datetime):
        new_date = pd.to_datetime(str(date))
        new_date_list.append(new_date.strftime('%Y.%m.%d'))
    return new_date_list
