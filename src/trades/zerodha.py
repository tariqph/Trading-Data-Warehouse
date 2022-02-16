import logging
import pandas as pd
logging.basicConfig(level=logging.DEBUG)
from access_token import access_token
import datetime

time = datetime.datetime.now()

kite = access_token()

data = kite.instruments()
data = pd.DataFrame(data)
print(data.head())
data.to_csv('Input/Data_Complete.csv')