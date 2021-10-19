import logging
from kiteconnect import KiteConnect
import pandas as pd
logging.basicConfig(level=logging.DEBUG)
import pyotp

import datetime

time = datetime.datetime.now()

from configparser import ConfigParser
parser = ConfigParser()

filename = 'database.ini'
section = 'zerodha'
# read config file
parser.read(filename)

db = {}
if parser.has_section(section):
	params = parser.items(section)
	for param in params:
		db[param[0]] = param[1]



kite = KiteConnect(api_key=db['api_key'])

# data = kite.generate_session(request_token, api_secret=api_secret)
kite.set_access_token(db['access_token'])



# kite = KiteConnect(api_key=api_key)

# # data = kite.generate_session(request_token, api_secret=api_secret)
# kite.set_access_token(access_token)


# order_id = kite.place_order(tradingsymbol="BANKNIFTY21OCT32600PE",
#                             price = 10,
#                                 exchange=kite.EXCHANGE_NFO,
#                                 transaction_type=kite.TRANSACTION_TYPE_SELL,
#                                 quantity=25,
#                                 variety=kite.VARIETY_AMO,
#                                 order_type=kite.ORDER_TYPE_LIMIT,
#                                 product=kite.PRODUCT_NRML)

# kite.cancel_order(variety=kite.VARIETY_AMO,order_id = 211004103164673)

# # print(kite.positions())
# print(len(kite.order_history(order_id = 211005203214480 )))
# print(kite.order_history(order_id = 211005203214480 )[len(kite.order_history(order_id = 211005203214480))-1]['status'])
# print(kite.order_trades(order_id = 211005203214480))

data = kite.instruments()
data = pd.DataFrame(data)
data = data[data['name'] == 'NIFTY BANK']
data.to_csv('Data_all2.csv')