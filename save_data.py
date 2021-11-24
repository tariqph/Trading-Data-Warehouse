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
    print(len(ticks))
    # Callback to receive ticks.
    # logging.debug("Ticks: {}".format(ticks))
    
    # global last_traded_strike, trade_count
    
    # print(ticks[0]['timestamp'].hour,ticks[0]['timestamp'].minute,ticks[0]['timestamp'].second,
    #        ticks[0]['last_price'], round(ticks[0]['last_price']/100)*100)
    
    
def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe([260105,272376070,272086278,271673350])


    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL,[260105,272376070,272086278,271673350])

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