# Adding paths to module used
import os
import sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","trades"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","publish_database"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","access_config"))
sys.path.append(fpath)

from os import times
from celery import Celery
from time import sleep, time
import datetime
from config import config
import psycopg2
import json
from json.decoder import JSONDecodeError
from pub import pub

list_req = ["instrument_token","last_price","volume","timestamp","oi"]

def flatten(ticks):
    next = []
    for token,tick in ticks.items():
        new_d = {}
        for k,v in tick.items():
            if k in list_req:
                if(k == 'timestamp'):
                    new_d[(k)] = str(v)
                else:
                    new_d[k] = v
        for w,j in tick['ohlc'].items():
            new_d[w] = j
        for i in list_req:
            if i not in new_d.keys():
                new_d[k] = "null" 
        next.append((new_d))
    return next

def union(prev,next,timestamp):
    ''' This function takes the ticks data for two consecutive timestamps. Since the ticks are only recieved
    when data changes so the prev data is rolled forwarded to the next timestamp data for the instruments
    which didn't recieve data in next'''
    
    for token, tick in prev.items():
        if int(token) not in next:
            next[token] = tick
    for token, tick in next.items():
        next[token]['timestamp'] = str(timestamp)

    return next


app = Celery('tasks', broker = 'amqp://guest:guest@localhost:5672//', backend='db+sqlite:///db.sqlite3')
app.conf.task_routes = {'feed.tasks.*': {'queue': 'stock1'}}

@app.task
def insert_db(ticks,
            #   prev_timestamp,next_timestamp,prev,next, 
              tablename, filename_data, filename_timestamp):
    ''' 
    Function to insert tick data into the database every minute. It receives a timestamp 
    and two ticker dictionaries. Two ticker dictionaries are used to ensure that the tick data for the 
    minute mark is not missed as the websocket recieves data when anything changes
    '''
    prev = {}
    next = {}
    prev_timestamp = '0'
    
    with open(filename_data, 'r') as infile:
        try:
            prev = json.load(infile)
        except JSONDecodeError:
            pass
        
    with open(filename_timestamp, 'r') as infile:
        try:
            prev_timestamp = json.load(infile)
        except JSONDecodeError:
            pass
     
    next_timestamp = ticks[0]['timestamp']
    # print(prev_timestamp)
    for tick in ticks:
        next[tick['instrument_token']] = tick
    
    # Check if the insert requirements are met before proceeding
    if(prev == {}):
        print('returning')
        
        next_tmp = next.copy()
        for token, tick in next.items():
            if 'volume' in tick.keys():
                if(tick['volume'] == 0):
                    next_tmp.pop(token)
        next = next_tmp.copy()
        
        with open(filename_data, 'w') as outfile:
            try:
                json.dump(next, outfile)
            except JSONDecodeError:
                pass
    
        with open(filename_timestamp, 'w') as outfile:
            try:
                 json.dump(next_timestamp, outfile)
            except JSONDecodeError:
                pass
      
        return
    
    # Convert string to datetime object for easy manipulation
    next_timestamp = datetime.datetime.strptime(next_timestamp,'%Y-%m-%dT%H:%M:%S')
    if(prev_timestamp !=  '0'):
        if('T' in prev_timestamp):
            prev_timestamp = datetime.datetime.strptime(prev_timestamp,'%Y-%m-%dT%H:%M:%S')
        else:
            prev_timestamp = datetime.datetime.strptime(prev_timestamp,'%Y-%m-%d %H:%M:%S')
            
    next = union(prev,next,next_timestamp)
    next_flat = flatten(next)
    print("stock1",len(next_flat))
    # print(next)

    print(prev_timestamp, next_timestamp)
    # logging.info(str(prev_timestamp), str(next_timestamp))
    
    
    # Insert data at the minute mark
    if(next_timestamp.second == 0):                        
        print('here1')
        for ticker in next_flat:
            ticker = json.dumps(ticker).encode('utf-8')
            # print(ticker)
            pub(ticker,"zerodha-332309", "ticker_data")
        # logging.info('here 1')
        
        
        for token,tick in next.items():
            continue
            
    
    # If the minute mark is missed in websocket response then the ticker 
    # did not change from the previous so used that for minute mark data
    elif((prev_timestamp != '0') and (next_timestamp.minute - prev_timestamp.minute)!=0 and
         (prev_timestamp.second)!=0):
        print('here2')
        for ticker in next_flat:
            ticker = json.dumps(ticker).encode('utf-8')
            # print(ticker)
            pub(ticker,"zerodha-332309", "ticker_data")
        
    
    with open(filename_data, 'w') as outfile:
        try:
            json.dump(next, outfile)

        except JSONDecodeError:
            pass
    
    with open(filename_timestamp, 'w') as outfile:
        try:
             json.dump(str(next_timestamp), outfile)
        except JSONDecodeError:
            pass
    
    return
