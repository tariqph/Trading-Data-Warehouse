from os import times
from celery import Celery
from time import sleep
import datetime
from config import config
import psycopg2
import json
from json.decoder import JSONDecodeError


app = Celery('tasks', broker = 'amqp://guest:guest@localhost:5672//', backend='db+sqlite:///db.sqlite3')

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
            
    
  
    # Establishing connection with database
    params = config()
    # connect to the PostgreSQL serve
    connection = psycopg2.connect(**params)
    connection.autocommit = True

    # cursor object for database
    cursor = connection.cursor()
    
    # SQL to insert for instruments and indices
    postgres_insert_query = f""" INSERT INTO {tablename} (date_time, instrument_token, 
                            ltp, volume, ohlc,open_interest) 
                            VALUES (%s,%s,%s, %s,%s,%s)"""
                            
    postgres_insert_query_index = f""" INSERT INTO index_data (date_time, instrument_token, 
                            ltp, ohlc) 
                            VALUES (%s,%s,%s,%s)"""

    # Insert data at the minute mark
    if(next_timestamp.second == 0):                        
        print('here1')
        for token,tick in next.items():
            # print(tick.keys())
            if('volume' in tick.keys()):
                record_to_insert = (next_timestamp, tick['instrument_token'], tick['last_price'],
                                    tick['volume'],json.dumps(tick['ohlc']), tick['oi']
                                    )
                cursor.execute(postgres_insert_query, record_to_insert)
            else:
                record_to_insert = (next_timestamp, tick['instrument_token'], tick['last_price'],
                                    json.dumps(tick['ohlc'])
                                    )
                cursor.execute(postgres_insert_query_index, record_to_insert)
    
    # If the minute mark is missed in websocket response then the ticker 
    # did not change from the previous so used that for minute mark data
    elif((prev_timestamp != '0') and (next_timestamp.minute - prev_timestamp.minute)!=0 and
         (prev_timestamp.second)!=0):
        print('here2')
        next_timestamp_insert = next_timestamp - datetime.timedelta(seconds = next_timestamp.second)
        for token,tick in prev.items():
    
            if('volume' in tick.keys()):
                record_to_insert = (next_timestamp_insert, tick['instrument_token'], tick['last_price'],
                                    tick['volume'],json.dumps(tick['ohlc']), tick['oi']
                                    )
                cursor.execute(postgres_insert_query, record_to_insert)
            else:
                record_to_insert = (next_timestamp_insert, tick['instrument_token'], tick['last_price'],
                                    json.dumps(tick['ohlc'])
                                    )
                cursor.execute(postgres_insert_query_index, record_to_insert)
    
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


                          
                # record_to_insert = (next_timestamp, tick['instrument_token'], tick['last_price'],
                #                     tick['volume'],json.dumps(tick['ohlc']), tick['oi'])
                # cursor.execute(postgres_insert_query, record_to_insert)