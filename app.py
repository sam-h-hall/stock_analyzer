from flask import Flask, request
from flask_cors import CORS
from time_series_functions import index
# from flask_socketio import SocketIO
from flask_sock import Sock
from flask_apscheduler import APScheduler
import json
import requests
import dotenv
import websocket
import requests
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from pprint import pformat
import math

api_key = dotenv.get_key(".env", "API_KEY_12_DATA")
exchanges = ['nyse', 'nasdaq']
some_data = ""


class Config:
    SCHEDULER_API_ENABLED = True


app = Flask(__name__)
sock = Sock(app)
app.config.from_object(Config())

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# @scheduler.task("interval", id="do_thing", seconds=5, misfire_grace_time=900)


def get_all_symbols():
    all_symbols = []
    all_symbols_filepath = "./json_data/reference_data/time_series_data/all_symbols.json"
    with open(all_symbols_filepath, "r") as file:
        all_symbols = json.load(file)
    return all_symbols


all_symbols = get_all_symbols()
all_symbols_str = ",".join(all_symbols)


class Time_Series:
    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                print(f"!>> Could not check for existence of table => {err.response['Error']['Code']}: \n{err.response['Error']['Message']}")
                raise
        else:
            self.table = table
        return exists

    def create_table(self, table_name):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttribueName': 'symbol', 'KeyType': 'HASH'}
                    # sort key(optional)
                    # {'AttribueName': 'symbol', 'KeyType': 'HASH'}
                ],
                AttributeDefinition=[
                    {'AttributeName': 'symbol', 'AttributeType': 'S'}
                ]
                # not sure
                # ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 10}
            )
            self.table.wait_until_exists()
        except ClientError as err:
            print(f"Could not create table: {table_name}")
        else:
            return self.table

    def add_symbol_data(self, data):
        try:
            self.table.put_item(
                Item={**data}
            )
            print("Item added")
        except ClientError as err:
            print(f"!>> Could not write to db => {err.response['Error']['Code']}:\n{err.response['Error']['Message']}")
            raise

    def update_symbol_data(self, symbol, new_data):

        get_existing_data = self.get_symbol_data(symbol)
        if get_existing_data == {}:
            self.add_symbol_data(new_data)
            return

        existing_data = get_existing_data['time_series']
        t_series_data = new_data['time_series']

        for date_key in t_series_data.keys():
            print("\n\nKey: ", date_key)
            print("Retrieved: ", new_data)
            print(t_series_data.keys())

            def expressionAttributeValues():
                if date_key in existing_data.keys():
                    return {':newData': {**t_series_data[date_key], **existing_data[date_key]}}
                else:
                    return {':newData': t_series_data[date_key]}

            try:
                response = self.table.update_item(
                    Key={'symbol': symbol},
                    UpdateExpression="set time_series.#date_key = :newData",
                    ExpressionAttributeNames={'#date_key': date_key},
                    ExpressionAttributeValues=expressionAttributeValues(),
                    ReturnValues="UPDATED_OLD"
                )
            except ClientError as err:
                print(f"!>> Could not update item in db=> {err.response['Error']['Code']}:\n{err.response['Error']['Message']}")
            else:
                print(f"\n >>Added data to date:{date_key}<<\n")

    def get_symbol_data(self, symbol):
        try:
            symbol_data = {}
            response = self.table.get_item(Key={'symbol': symbol})
        except ClientError as err:
            print(f"!>> Could not get requested symbol => {err.response['Error']['Code']}:\n{err.response['Error']['Message']}")
        else:
            if 'Item' in response:
                return response["Item"]
            else:
                return {}

    def list_tables(self):
        try:
            tables = []
            for table in self.dyn_resource.tables.all():
                print(table.name)
                tables.append(table)
        except ClientError as err:
            print(f"!>> Could not list tables => {err.response['Error']['Code']}:\n{err.response['Error']['Message']}")
            raise
        else:
            return tables


def run_scenario(table_name, dyn_resource):
    t_series = Time_Series(dyn_resource)
    print(f"Tables: {t_series.list_tables()}")
    print(f"Table exists: {t_series.exists(table_name)}")


def handle_symbol_batches(all_symbols_list):
    _batch_group_size = 5
    _batch_group_count = math.ceil(len(all_symbols) / _batch_group_size)
    symbol_batches = []

    range_low = 0
    range_high = _batch_group_size

    for group in range(_batch_group_count):
        batch = all_symbols_list[range_low:range_high]
        symbol_batches.append(batch)
        range_low += 5
        range_high += 5

    return symbol_batches[:2]


# @scheduler.task("interval", id="do_thing", seconds=5, misfire_grace_time=900)
# @scheduler.task('cron', id='do_job_1', hour=0, minute=5)
# @scheduler.task('cron', id='do_job_1', hour=0, minute="*")

def get_time_series_all():
    global all_symbols

    print("Starting cron job")
    outputsize = 5
    url = "https://api.twelvedata.com/time_series"
    params = {"apikey": api_key, "country": "United States", "outputsize": outputsize, "interval": '5min', "symbol": ""}
    table_name = "test-table-sam"

    t_series = Time_Series(boto3.resource('dynamodb'))
    t_series.list_tables()
    t_series.exists(table_name)

    for idx, batch in enumerate(handle_symbol_batches(all_symbols)):
        if batch == []:
            print("No more batch data")
            return
        print(f"Beginnin job for batch {idx + 1}\n{batch}")
        params['symbol'] = ",".join(batch)
        print("Batch param: ", params['symbol'])
        response = requests.request("GET", url, params=params)

        response_data = json.loads(response.text)
        print('AHHHHHHH: ', response_data)

        for key in response_data.keys():
            data_to_send = {}
            data_to_send['symbol'] = key
            data_to_send['time_series'] = {}
            data_to_send['current_status'] = response_data[key]['status']

            data_to_send['meta'] = response_data[key]['meta']

            response_data[key]['time_series'] = response_data[key]['values']
            response_data[key]['values'].pop()

            for t_series_data in response_data[key]['time_series']:

                split_datetime = t_series_data['datetime'].split()
                date = split_datetime[0]
                timestamp = split_datetime[1]

                date_exists = data_to_send['time_series'].get(date)

                if date_exists:
                    data_to_send['time_series'][date].update({**data_to_send['time_series'][date], timestamp: {**t_series_data}})
                else:
                    data_to_send['time_series'][date] = {timestamp: {**t_series_data}}

            t_series.update_symbol_data(data_to_send['symbol'], data_to_send)

        print("DONE")


kwargs = {"trigger": "interval", "minutes": 5}
scheduler.add_job('get_all_t_series', get_time_series_all, **kwargs)

# def websocket_client():
# ws = websocket.WebSocket()
# link = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={api_key}"
# print(link)
# ws.connect(link)
# ws.send({
#     "action":"subscribe",
#     "params": {
#         "symbols": "BTC/USD"
#     }
# })
# print("thing: ", ws.recv())
