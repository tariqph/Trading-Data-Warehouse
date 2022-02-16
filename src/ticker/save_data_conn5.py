# Adding paths to module used
import os
import sys
import datetime
from json.decoder import JSONDecodeError
from configparser import ConfigParser
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","trades"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","publish_database"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","access_config"))
sys.path.append(fpath)
from access_token import access_token

import logging
from kiteconnect import KiteTicker
from publish_pubsub_1 import insert_db
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
import glob

import yaml

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
    	config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')

# Paths for cache for prev ticker and timestamp
# temp_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","temp_cache"))
# filename_data = os.path.abspath(os.path.join(temp_file_path,"prev_2.txt"))
# filename_timestamp = os.path.abspath(os.path.join(temp_file_path,"prev_timestamp_2.txt"))

filename_data = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                             config['cache_files']['prev_5']))
filename_timestamp = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                             config['cache_files']['prev_timestamp_5']))

# Clear the previous ticks file
# open(filename_data, 'w').close()
# open(filename_timestamp, 'w').close()

# Get the List of Instrument tokens
# output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","Data/Output"))
# path = os.path.abspath(os.path.join(output_path,"all_tokens_ex_curr.csv"))  # use your path

path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                    config['output_files']['merged_tokens']['all_instruments_ex_currencies']))

# all_files = glob.glob(path + "/*.csv")

# mod_files = []
# for file in all_files:
#     # if 'stock' in file:
#     mod_files.append(file)
       
# li = []
# for filename in mod_files:
#     df = pd.read_csv(filename, index_col=None, header=0)
#     li.append(df)

data = pd.read_csv(path)
instrument_list = data['instrument_token'].to_list()

n = len(instrument_list)
instrument_list = instrument_list[int(3*n/5):int(4*n/5)]

high_low_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                    config['high_low_files']['conn_5']))

exec_date_filename = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",
                                 "..","last_execution_date.txt"))

# print(exec_date_filename)
with open(exec_date_filename, 'a+') as infile:
	try:
		# prev = json.load(infile)
		infile.seek(0)
		a = infile.readline()
		print(a)
	except JSONDecodeError:
		print("exception")
		pass
# print(a)
date = datetime.date.today()
date_exec = datetime.datetime.strptime(a, "%Y-%m-%d")

if(date != date_exec.date()):
    high_low_file_parser = ConfigParser()
    high_low_file_parser.read(high_low_path)
    
    for instrument in instrument_list:
        # db[instrument] = {'high' : 0, 'low': 100000}
        if str(instrument) not in high_low_file_parser.sections():
            high_low_file_parser.add_section(str(instrument))
        high_low_file_parser.set(str(instrument),'high', '0')
        high_low_file_parser.set(str(instrument),'low', '100000')

    # Clear the previous ticks file
    open(filename_data, 'w').close()
    open(filename_timestamp, 'w').close()


    # Writing the changes to trades file
 

    with open(high_low_path, 'w') as configfile:
        high_low_file_parser.write(configfile)


table_name = 'stockdata1_test'
# instrument_list = instrument_list[0:10]
print("conn2")
print(len(instrument_list))


# Filename for the connection details
# filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","database.ini"))
filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                        config['access_files']['api_two']))
kws = access_token(filename = filename, type = 'kws')

# kws = KiteTicker(db['api_key'], db['access_token'])

# Message broker queue name
count = 0
queue = config['rabbitmq']['queues']['five']
def on_ticks(ws, ticks):
    # print(len(ticks),(ticks[0]['timestamp']), len(ticks))
    # if(ticks[0]['timestamp'].second == 0):
    # 	print('stock1:' ,(ticks[0]['timestamp']), len(ticks))
    print("conn_5",ticks[0]['exchange_timestamp'],len(ticks))

    if(ticks[0]['exchange_timestamp'].hour < int(config['start_connection']['stock']['hour']) ):
        return
    
    if(ticks[0]['exchange_timestamp'].hour <= int(config['start_connection']['stock']['hour']) 
    and ticks[0]['exchange_timestamp'].minute < int(config['start_connection']['stock']['minute'])-1):
        return   

    insert_db.apply_async(args = (ticks,
                    # prev_timestamp,ticks[0]['timestamp'], prev_dict, next_dict, 
                    table_name, filename_data,
                    filename_timestamp,high_low_path), queue=queue)
    # if(ticks[0]['exchange_timestamp'].minute == 45):
    #     on_close(ws,"100", "time-out closed")
    if(ticks[0]['exchange_timestamp'].hour == int(config['end_connection']['stock']['hour']) 
    and ticks[0]['exchange_timestamp'].minute == int(config['end_connection']['stock']['minute']) 
    and ticks[0]['exchange_timestamp'].second == int(config['end_connection']['stock']['second']) ):
        on_close(ws,"100", "time-out closed - exchange closed")

    
def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens 
    ws.subscribe(instrument_list)

    # Set to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL,instrument_list)

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()