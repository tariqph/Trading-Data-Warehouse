import logging
from kiteconnect import KiteTicker
import orders

logging.basicConfig(level=logging.DEBUG)

# global variables

last_traded_strike = 0
trade_count = 0


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
    # print(ticks)
    # Callback to receive ticks.
    # logging.debug("Ticks: {}".format(ticks))
    
    global last_traded_strike, trade_count
    
    print(ticks[0]['timestamp'].hour,ticks[0]['timestamp'].minute,ticks[0]['timestamp'].second,
           ticks[0]['last_price'], round(ticks[0]['last_price']/100)*100)
    
    price = ticks[0]['last_price']
    strike = round(ticks[0]['last_price']/100)*100
    
    
    # if(ticks[0]['timestamp'].minute == 17):
    #     kws.close()
    
    if(ticks[0]['timestamp'].minute == 44):
        price = price + 340
        strike = round(price/100)*100
        
    
    if(last_traded_strike != 0 and ticks[0]['timestamp'].minute == 44 and ticks[0]['timestamp'].second == 0):
        if(abs(last_traded_strike - price) >= 300):
            print('second')
            print(strike)
            call = "BANKNIFTY21OCT" + str(last_traded_strike) + 'CE'
            put = "BANKNIFTY21OCT" + str(last_traded_strike) + 'PE'
            symbols = [call, put]
            orders.place_order(symbols,last_traded_strike,['buy','buy'])
            
            call1 = "BANKNIFTY21OCT" + str(strike) + 'CE'
            put1 = "BANKNIFTY21OCT" + str(strike) + 'PE'
            symbols1 = [call1, put1]
            last_traded_strike, trade_count =  orders.place_order(symbols1,strike,['sell','sell'])
            
            
        
    if(ticks[0]['timestamp'].hour == 16 and ticks[0]['timestamp'].minute == 43
       and ticks[0]['timestamp'].second == 0):
        print('here')
        print(last_traded_strike)
        print(trade_count)
        call = "BANKNIFTY21OCT" + str(strike) + 'CE'
        put = "BANKNIFTY21OCT" + str(strike) + 'PE'
        symbols = [call,put]
        last_traded_strike, trade_count =  orders.place_order(symbols,strike,['sell','sell'])
        # last_traded_strike, trade_count =  orders.place_order(put,strike,'sell')
        


def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe([260105])

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, [260105])

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