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
import json
logging.basicConfig(level=logging.DEBUG)
from pub import pub
import pandas as pd
# global variables
data = pd.read_csv('G:\DS - Competitions and projects\Zerodha\Data/Output/Final/final_token_stock_one_month.csv')
instrument_list = data['instrument_token'].to_list()

last_traded_strike = 0
trade_count = 0
list_req = ["instrument_token","last_price","volume","buy_quantity", "sell_quantity","timestamp","oi"]

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
    print(len(ticks))
    # print(ticks[0])
    timestamps = set()
    for tick in ticks:
        timestamps.add(tick['timestamp'])
    
    print(timestamps)
        
    # next = []
    # for tick in ticks:
    #     new_d = {}
    #     for k,v in tick.items():
    #         if k in list_req:
    #             if(k == 'timestamp' or k=='buy_quantity' or k=='sell_quantity'):
    #                 new_d[(k)] = str(v)
    #             else:
    #                 new_d[k] = v
    #     for w,j in tick['ohlc'].items():
    #         new_d[w] = j
    #     for i in list_req:
    #         if i not in new_d.keys():
    #             new_d[k] = "null" 
    #     next.append((new_d))
        
    # print(ticks[0]['timestamp'])
    # for n in next:
    #     n = json.dumps(n)
    #     n = bytes(n, 'utf-8')
    #     if(ticks[0]['timestamp'].second % 10 ==0):
            # pub(n,"zerodha-332309", "ticker_data")
            # print("yes")
            # print(n)
        # print(ticks[0]['timestamp'])
        # print(n)
        
    # if(ticks[0]['timestamp'].second % 10 == 0):
    #     pub(next,"zerodha-332309", "tick_store")    
        
    # print(next)
    # Callback to receive ticks.
    # logging.debug("Ticks: {}".format(ticks))
    
    # Commented out 
    # global last_traded_strike, trade_count
    
    # print(ticks[0]['timestamp'].hour,ticks[0]['timestamp'].minute,ticks[0]['timestamp'].second,
    #        ticks[0]['last_price'], round(ticks[0]['last_price']/100)*100)
    
    # price = ticks[0]['last_price']
    # strike = round(ticks[0]['last_price']/100)*100
    
    
    # # if(ticks[0]['timestamp'].minute == 17):
    # #     kws.close()
    # if(ticks[0]['timestamp'].minute == 44):
    #     price = price + 340
    #     strike = round(price/100)*100
        
        
    
    # if(last_traded_strike != 0 and ticks[0]['timestamp'].minute == 44 and ticks[0]['timestamp'].second == 0):
    #     if(abs(last_traded_strike - price) >= 300):
    #         print('second')
    #         print(strike)
    #         call = "BANKNIFTY21OCT" + str(last_traded_strike) + 'CE'
    #         put = "BANKNIFTY21OCT" + str(last_traded_strike) + 'PE'
    #         symbols = [call, put]
    #         orders.place_order(symbols,last_traded_strike,['buy','buy'])
            
    #         call1 = "BANKNIFTY21OCT" + str(strike) + 'CE'
    #         put1 = "BANKNIFTY21OCT" + str(strike) + 'PE'
    #         symbols1 = [call1, put1]
    #         last_traded_strike, trade_count =  orders.place_order(symbols1,strike,['sell','sell'])
            
            
        
    # if(ticks[0]['timestamp'].hour == 16 and ticks[0]['timestamp'].minute == 43
    #    and ticks[0]['timestamp'].second == 0):
    #     print('here')
    #     print(last_traded_strike)
    #     print(trade_count)
    #     call = "BANKNIFTY21OCT" + str(strike) + 'CE'
    #     put = "BANKNIFTY21OCT" + str(strike) + 'PE'
    #     symbols = [call,put]
    #     last_traded_strike, trade_count =  orders.place_order(symbols,strike,['sell','sell'])
    #     # last_traded_strike, trade_count =  orders.place_order(put,strike,'sell')
        


def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(instrument_list)

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, instrument_list)

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