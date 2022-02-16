# Adding paths to module used
import os
import sys
from configparser import ConfigParser
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
import json
from json.decoder import JSONDecodeError
from pub import pub

import yaml

# Reading config from yaml file
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
    	config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')

list_req = ["instrument_token","last_price","volume_traded","exchange_timestamp","oi"]

def flatten(ticks,high_low_parser):
    next = []
    for token,tick in ticks.items():
        new_d = {}
        for k,v in tick.items():
            if k in list_req:
                if(k == 'exchange_timestamp'):
                    new_d[(k)] = str(v).replace('T',' ')
                else:
                    new_d[k] = v
        # print(new_d)
        for w,j in tick['ohlc'].items():
            if (w == 'high' or w == 'low'):
                new_d[w] = float(high_low_parser.get(str(token),str(w)))
            else:
                new_d[w] = j
        # for i in list_req:
        #     if i not in new_d:
        #         new_d[i] = "null" 
        next.append((new_d))
        # print(next)
    return next

def union(prev,next,timestamp):
    ''' This function takes the ticks data for two consecutive timestamps. Since the ticks are only recieved
    when data changes so the prev data is rolled forwarded to the next timestamp data for the instruments
    which didn't recieve data in next'''
    
    for token, tick in prev.items():
        if int(token) not in next:
            next[token] = tick
    for token, tick in next.items():
        next[token]['exchange_timestamp'] = str(timestamp)

    return next

host = config['rabbitmq']['host']
user = config['rabbitmq']['username']
password = config['rabbitmq']['password']
port = config['rabbitmq']['port']

app = Celery('tasks', broker = f'amqp://{user}:{password}@{host}:{port}//',
 backend='rpc:// ')
# app.conf.task_routes = {'feed.tasks.*': {'queue': 'conn_1'}}

@app.task
def insert_db(ticks,
            #   prev_timestamp,next_timestamp,prev,next, 
              tablename, filename_data, filename_timestamp,high_low_file):
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
     
    next_timestamp = ticks[0]['exchange_timestamp']
    # print(prev_timestamp)
    for tick in ticks:
        next[tick['instrument_token']] = tick
    
    high_low_parser = ConfigParser()
    
    high_low_parser.read(high_low_file)
    
    for token, tick in next.items():
        if( tick['last_price'] > float(high_low_parser.get(str(token),"high"))):
            high_low_parser.set(str(token),"high",str(tick['last_price']))
        if( tick['last_price'] < float(high_low_parser.get(str(token),"low"))):
            high_low_parser.set(str(token),"low",str(tick['last_price']))
    
    # with open(high_low_file, 'w') as configfile:
    #     high_low_parser.write(configfile)
        
    # Check if the insert requirements are met before proceeding
    if(prev == {}):
        print('returning')
    
        
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
    next_timestamp = next_timestamp.replace('T',' ')
    prev_timestamp = prev_timestamp.replace('T',' ')

    next_timestamp = datetime.datetime.strptime(next_timestamp,'%Y-%m-%d %H:%M:%S')
    if(prev_timestamp !=  '0'):
        if('T' in prev_timestamp):
            prev_timestamp = datetime.datetime.strptime(prev_timestamp,'%Y-%m-%dT%H:%M:%S')
        else:
            prev_timestamp = datetime.datetime.strptime(prev_timestamp,'%Y-%m-%d %H:%M:%S')
            
    next = union(prev,next,next_timestamp)
    
    next_flat = flatten(next,high_low_parser)
    print("stock1",len(next_flat))
    # print(next)

    print(prev_timestamp, next_timestamp)
    # logging.info(str(prev_timestamp), str(next_timestamp))
    
    
    # Insert data at the minute mark
    if(next_timestamp.second == 0):                        
        print('here1')
        
        if(prev_timestamp == next_timestamp):
            for token,tick in next.items():
                    high_low_parser.set(str(token),"high",'0')
                    high_low_parser.set(str(token),"low",'100000')
            return
        
        for ticker in next_flat:
            ticker_json = json.dumps(ticker).encode('utf-8')
            # print(ticker)
            pub(ticker_json,config['gcloud']['project_name'], config['gcloud']['pubsub_topic'])
            
            high_low_parser.set(str(ticker['instrument_token']),"high",'0')
            high_low_parser.set(str(ticker['instrument_token']),"low",'100000')
        # logging.info('here 1')
        
        

            
    
    # If the minute mark is missed in websocket response then the ticker 
    # did not change from the previous so used that for minute mark data
    elif((prev_timestamp != '0') and (next_timestamp.minute - prev_timestamp.minute)!=0 and
         (prev_timestamp.second)!=0):
        print('here2')
        for ticker in next_flat:
            ticker_json = json.dumps(ticker).encode('utf-8')
            # print(ticker)
            pub(ticker_json,config['gcloud']['project_name'], config['gcloud']['pubsub_topic'])
        
            high_low_parser.set(str(ticker['instrument_token']),"high",'0')
            high_low_parser.set(str(ticker['instrument_token']),"low",'100000')
    
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
    
    # Commiting the high low of instruments to the file
    with open(high_low_file, 'w') as configfile:
        high_low_parser.write(configfile)
    
    return
