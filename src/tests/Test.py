# from celery import Celery
# from time import sleep
# # import winsound

# app = Celery('tasks', broker = 'amqp://guest:guest@localhost:5672//', backend='db+sqlite:///db.sqlite3')

# from configparser import ConfigParser
# parser = ConfigParser()

# parser.read('test.ini')

# @app.task
# def add_print(x):
#     sleep(5)
#     parser.set('new', 'key', 'value')	
    
#     with open('test.ini', 'w') as configfile:
        
#         parser.write(configfile)
#     duration = 1000  # milliseconds
#     freq = 440  # Hz
#     winsound.Beep(freq, duration)
    
#     print(x*2)

# import pandas as pd
# indices_curr_list = pd.read_csv("index_currency_list.csv")

# indices_curr_list = indices_curr_list[indices_curr_list['name'] != 'USDINR']
# print(indices_curr_list)
# temp = indices_curr_list[indices_curr_list['period'] == 'Weekly']
# total_instruments = pd.read_csv("Data_Complete.csv")
# indexes = total_instruments[total_instruments['exchange'] == 'NSE']
# indexes = indexes[indexes['tradingsymbol'].isin(temp['symbol'])]

# print(indexes)

# import pandas as pd
# import datetime

# def expiry_days_symbol(months, weeks):
#     today = datetime.date.today()

#     month_symbol = []
#     for date in months:
#         if(date.date() < today):
#             continue
#         month_symbol.append(str(date.strftime('%y')).upper() + str(date.strftime('%b')).upper())
        
#     week_symbol = []

#     for date in weeks:
#         if(date.date() < today):
#             continue
#         week_symbol.append(str(date.strftime('%y')).upper() +
#                         str(date.strftime('%b')).upper()[0] +
#                         str(date.strftime('%d')).upper() )
#      # Hard coded Data   
#     return month_symbol[0:3], week_symbol[0:3]

# index_expiry = pd.read_csv('Input_Data/expiry_index.csv')

# index_weeks = index_expiry[index_expiry['Type'] == 'W']
# index_weeks.loc[:,'Dates'] = pd.to_datetime(index_weeks.loc[:,'Dates'])
# index_weeks = index_weeks['Dates'].tolist()


# index_months = index_expiry[index_expiry['Type'] == 'M']
# index_months.loc[:,'Dates'] = pd.to_datetime(index_months.loc[:,'Dates'])
# index_months = index_months['Dates'].tolist()

# # print(weeks, months)
# month_symbol_index, week_symbol_index = expiry_days_symbol(index_months,index_weeks)
# print(month_symbol_index, week_symbol_index)


# # Insert all the instruments in database
# import pandas as pd
# import glob
# from config import config
# import psycopg2

# path = r'G:\DS - Competitions and projects\Zerodha\Data\\Final' # use your path
# all_files = glob.glob(path + "/*.csv")

# li = []

# for filename in all_files:
#     df = pd.read_csv(filename, index_col=None, header=0)
#     li.append(df)

# frame = pd.concat(li, axis=0, ignore_index=True)

# print(frame.shape[0])
# print(frame.head())
# params = config()

# user = params['user']
# password = params['password']     
# print(user, password) 
# from sqlalchemy import create_engine
# engine = create_engine(f'postgresql://{user}:{password}@localhost:5432/test')
# frame.to_sql('instrument_list', engine, if_exists= 'replace')

import json
# d = "fgfgsgs"
# json.dump(d, open("try.txt",'w'))
# # d2 = json.load(open("try.txt"))
# # print(d2)

# # d2 = {}
# from json.decoder import JSONDecodeError

# with open("trgfy.txt", 'w+') as infile:
#     try:
#         d2 = json.load(infile)
#         # data = old_data + obj
#         # json.dump(data, outfile)
#     except JSONDecodeError:
#         pass

# print(d2)

# a = "2021-11-11 12:41:24"

# import datetime
# next_timestamp = datetime.datetime.strptime(a,'%Y-%m-%d %H:%M:%S')
# # print(a)
# open('prev1212.txt', 'w').close()
# open('prev_timestamp212.txt', 'w').close()

# import glob
# path = r'Data/Final' # use your path
# all_files = glob.glob(path + "/*.csv")

# print(len(all_files),all_files)

# mod_files = []
# for file in all_files:
#     if 'stock' not in file:
#        mod_files.append(file)

# print(len(mod_files),mod_files) 

# import pandas as pd
# li = []
# for filename in mod_files:
#     df = pd.read_csv(filename, index_col=None, header=0)
#     li.append(df)

# frame = pd.concat(li, axis=0, ignore_index=True)

# print(frame.head())
# instruments = frame['instrument_token'].tolist()
# print(len(instruments))
# # with open("prev.txt", 'w') as outfile:
#         try:
#             json.dump(None, outfile)

#         except JSONDecodeError:
#             pass
# with open("prev_timestamp.txt", 'w') as outfile:
#         try:
#              json.dump(None, outfile)
#         except JSONDecodeError:
#             pass

# text = '[{"tr_time_str": "2021-11-17 19:00:40", "first_name": "Stephane", "last_name": "Ulrike", "city": "Arlington", "state": "CT", "product": "Product 5 XL", "amount": 687.85},{"tr_time_str": "2021-11-17 19:00:40", "first_name": "John", "last_name": "Doe", "city": "Arlington", "state": "CT", "product": "Product 5 XL", "amount": 687.85}]'
# a = json.dumps(text)
# print(type(a))

# import os
# import datetime



# now = datetime.datetime.now()
# date_time = now.strftime("%m-%d-%Y-%H-%M-%S")

# # Create a cloud Dataflow job
# job_name = "dfsql-hil8s363-kwdgvpuh" + date_time

# gc_command = f'gcloud dataflow sql query "SELECT tr.* FROM pubsub.topic.`zerodha-332309`.ticker_data  as tr" '\
# f'--job-name {job_name} ' \
# '--region asia-south1 '\
# '--bigquery-write-disposition write-empty '\
# '--bigquery-project zerodha-332309 '\
# '--bigquery-dataset ticker_data --bigquery-table new_day_1'

# os.system(gc_command)
import threading
import os
class myThread (threading.Thread):
	def __init__(self, command):
		threading.Thread.__init__(self)
		self.cmd = command

	def run(self):
		print ("Starting " + self.cmd)
		os.system(self.cmd)
		print ("Exiting " + self.cmd)

# Commands to run parallely
lstCmd=['cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_f  worker -Q fando --concurrency=1  --loglevel=info -P  eventlet -n worker1@%h', 
		'cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_1  worker -Q stock1 --concurrency=1  --loglevel=info -P  eventlet -n worker2@%h',
		'cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_2  worker -Q stock2 --concurrency=1  --loglevel=info -P  eventlet -n worker3@%h'
		]   

# Create new threads
thread1 = myThread(lstCmd[0])
thread2 = myThread(lstCmd[1])
thread3 = myThread(lstCmd[2])
# thread4 = myThread(lstCmd[3])
# thread5 = myThread(lstCmd[4])
# thread6 = myThread(lstCmd[5])


# Start new Threads
thread1.start()
thread2.start()
thread3.start()
# thread4.start()
# thread5.start()
# thread6.start()
