# Adding paths to module used
import os
import sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","trades"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","publish_database"))
sys.path.append(fpath)


import logging
from kiteconnect import KiteTicker
import orders
from insert_database import insert_db

logging.basicConfig(level=logging.DEBUG)

# global variables

last_traded_strike = 0
trade_count = 0

instrument_list = [260617,264457,256265,268041,265993,
                  273673,263433,260105,257289,257545
                  ]

prev_dict = {}
next_dict = {}
prev_timestamp = 0
# Initialise

from configparser import ConfigParser
parser = ConfigParser()
parser1 = ConfigParser()

filename = 'database.ini'
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
    print((ticks[0]['timestamp']))
    
    # Callback to receive ticks.
    # logging.debug("Ticks: {}".format(ticks))
    
    global next_dict, prev_dict, prev_timestamp
    
    for tick in ticks:
        next_dict[tick['instrument_token']] = tick
        
    insert_db.delay(prev_timestamp,ticks[0]['timestamp'], prev_dict, next_dict,'zerodha_data_2')
    
    prev_timestamp = ticks[0]['timestamp']
    prev_dict = next_dict.copy()
    
    # print(ticks[0]['timestamp'].hour,ticks[0]['timestamp'].minute,ticks[0]['timestamp'].second,
    #        ticks[0]['last_price'], round(ticks[0]['last_price']/100)*100)
    
    
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