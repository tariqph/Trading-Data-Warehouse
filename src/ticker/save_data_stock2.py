# Adding paths to module used
import os
import sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","trades"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","publish_database"))
sys.path.append(fpath)

import logging
from kiteconnect import KiteTicker
from publish_pubsub_2 import insert_db
logging.basicConfig(level=logging.DEBUG)
import pandas as pd

# global variables

last_traded_strike = 0
trade_count = 0

# filename_data = 'prev_2.txt'
# filename_timestamp = 'prev_timestamp_2.txt'
temp_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","temp_cache"))

filename_data = os.path.abspath(os.path.join(temp_file_path,"prev_2.txt"))
filename_timestamp = os.path.abspath(os.path.join(temp_file_path,"prev_timestamp_2.txt"))

# Clear the previous ticks file
open(filename_data, 'w').close()
open(filename_timestamp, 'w').close()

# Get the List of Instrument tokens

# data = pd.read_csv('Data/Final/final_token_stock_three_month.csv')
data = pd.read_csv('G:\DS - Competitions and projects\Zerodha\Data/Output/Final/final_token_stock_three_month.csv')

instrument_list = data['instrument_token'].to_list()

# Table name of the db to be inserted
table_name = 'stockdata2_test'
# instrument_list = instrument_list[0:20]
print(len(instrument_list))

# instrument_list = [256265,18258178]

prev_dict = {}
next_dict = {}
prev_timestamp = '0'
# Initialise

from configparser import ConfigParser
parser = ConfigParser()
parser1 = ConfigParser()

filename = "G:\DS - Competitions and projects\Zerodha\database.ini"
section = 'zerodha'
# read config file
parser.read(filename)

parser1.read('trades.ini')
try:
    last_traded_strike = parser1.getfloat('orders','strike')
    trade_count = parser1.getint('orders','trade_count')

except:
    print('Not traded yet')
    pass

db = {}
if parser.has_section(section):
	params = parser.items(section)
	for param in params:
		db[param[0]] = param[1]

kws = KiteTicker(db['api_key'], db['access_token'])

def on_ticks(ws, ticks):
    # print('stock2:' ,(ticks[0]['timestamp']), len(ticks))
    print(ticks[0]['timestamp'],len(ticks))
    # Callback to receive ticks.
    # logging.debug("Ticks: {}".format(ticks))
    
    # global next_dict, prev_dict, prev_timestamp
    
    # for tick in ticks:
    #     next_dict[tick['instrument_token']] = tick
        
    insert_db.apply_async(args = (ticks,
                    # prev_timestamp,ticks[0]['timestamp'], prev_dict, next_dict, 
                    table_name,filename_data,filename_timestamp),queue='stock2')
    

def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(instrument_list)


    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL,instrument_list)

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()