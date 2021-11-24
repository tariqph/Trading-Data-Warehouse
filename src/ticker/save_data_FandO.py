# Adding paths to module used
import os
import sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","trades"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","publish_database"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",".."))
sys.path.append(fpath)

import logging
from kiteconnect import KiteTicker
from publish_pubsub_f import insert_db
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
import glob


# filename_data = 'prev_f.txt'
# filename_timestamp = 'prev_timestamp_f.txt'

temp_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","temp_cache"))

filename_data = os.path.abspath(os.path.join(temp_file_path,"prev_f.txt"))
filename_timestamp = os.path.abspath(os.path.join(temp_file_path,"prev_timestamp_f.txt"))

# Clear the previous ticks file
open(filename_data, 'w').close()
open(filename_timestamp, 'w').close()

# Get the List of Instrument tokens
output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","Data\Output"))
path = os.path.abspath(os.path.join(output_path,"Final"))  # use your path
all_files = glob.glob(path + "/*.csv")

mod_files = []
for file in all_files:
    if 'curr' in file or 'index' in file:
       mod_files.append(file)
       
li = []

for filename in mod_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

data = pd.concat(li, axis=0, ignore_index=True)
instrument_list = data['instrument_token'].to_list()

# Table name of the db to be inserted
table_name = 'fando_test'
# instrument_list = instrument_list[0:100]
print(len(instrument_list))


prev_dict = {}
next_dict = {}
prev_timestamp = '0'
# Initialise

from configparser import ConfigParser
parser = ConfigParser()
parser1 = ConfigParser()

# filename = "G:\DS - Competitions and projects\Zerodha\database.ini"
filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","database.ini"))
section = 'zerodha'
# read config file
parser.read(filename)


db = {}
if parser.has_section(section):
	params = parser.items(section)
	for param in params:
		db[param[0]] = param[1]

kws = KiteTicker(db['api_key'], db['access_token'])

def on_ticks(ws, ticks):
    
    if(ticks[0]['timestamp'].minute == 0):
    	print('fando:' ,(ticks[0]['timestamp']), len(ticks))
    
    # Callback to receive ticks.
    # logging.debug("Ticks: {}".format(ticks))
    
    # Call the celery task    
    insert_db.apply_async(args =(ticks,
                    # prev_timestamp,ticks[0]['timestamp'], prev_dict, next_dict, 
                    table_name, filename_data, filename_timestamp), queue='fando')
    
 
    
    
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