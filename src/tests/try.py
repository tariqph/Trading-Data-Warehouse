# from Test import add_print
# from celery import Celery
# from time import sleep

# # # for i in range():
# # result = add_print.delay(55)

# # print("here")
# # print("here1")
# # print("here2")
# # while (True):
# #     sleep(0.5)
# #     print(result.status)
# #     if(result.status == "SUCCESS"):
# #         break

# import datetime
# import dateutil.relativedelta as relativedelta

# # string = '2021-10-25 16:16:21'

# # timestamp = datetime.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')

# # print(timestamp)
# today = datetime.date.today()
# rd = relativedelta.relativedelta(day = 31, weekday = relativedelta.TH(-1))
# month_last_thurs = today + rd

# num_days = today.weekday() - 3

# if(num_days <= 0):
# 	next_thurs = today + datetime.timedelta(days = abs(num_days))
# else:
# 	next_thurs = today + datetime.timedelta(days = (7 - num_days))

# print(next_thurs)
 
# import pandas as pd
# stock_list_input = "Input_Data/stock_list.csv"
# indices_list_input = "Input_Data/index_currency_list.csv"
# total_instruments_input = "Input_Data/Data_Complete.csv"
# index_expiry_input  = 'Input_Data/expiry_index.csv'
# curr_expiry_input = 'Input_Data/expiry_curr.csv'

# # # Get the current list of instruments from zerodha
# # data = kite.instruments()
# # data = pd.DataFrame(data)
# # data.to_csv(total_instruments_input)

# stock_list = pd.read_csv(stock_list_input)
# indices_curr_list = pd.read_csv(indices_list_input)

# indices_list = indices_curr_list[indices_curr_list['name'] != 'USDINR'].copy()
# curr_list = indices_curr_list[indices_curr_list['name'] == 'USDINR'].copy()

# curr_list.loc[:,"ltp"] = 75

# import pandas as pd

# data = pd.read_csv('Data/Final/final_token_stock_three_month.csv')

# instrument_token_list = data['instrument_token'].to_list()

# print((instrument_token_list))

# text = b'''{"20590082": {"tradable": true, "mode": "full", 
# "instrument_token": 20590082, "last_price": 152.0, "last_quantity": 
# 125, "average_price": 161.2, "volume": 13000, "buy_quantity": 4875, 
# "sell_quantity": 8000, "ohlc": {"open": 174.95, "high": 181.95, "low": 
# 146.2, "close": 176.45}, "change": -13.856616605270611, "last_trade_time": 
# "2021-11-15T13:24:03", "oi": 16125, "oi_day_high": 16250, "oi_day_low": 12125,
# "timestamp": "2021-11-15T13:25:52", "depth": {"buy": [{"quantity": 625, "price": 
# 150.0, "orders": 1}, {"quantity": 125, "price": 148.4, "orders": 1}, {"quantity": 
# 500, "price": 147.4, "orders": 1}, {"quantity": 250, "price": 145.45, "orders": 1}, 
# {"quantity": 125, "price": 144.2, "orders": 1}], "sell": 
# [{"quantity": 125, "price": 152.3, "orders": 1}, {"quantity": 125, "price": 152.35, "orders": 1}, 
# {"quantity": 125, "price": 152.55, "orders": 1}, {"quantity": 125, "price": 152.95, "orders": 1},
# {"quantity": 500, "price": 153.45, "orders": 1}]}}, 
# "20584194": {"tradable": true, "mode": "full", 
# "instrument_token": 20584194, "last_price": 109.45, "last_quantity": 125, "average_price": 106.58, 
# "volume": 17875, "buy_quantity": 6000, "sell_quantity": 12875, "ohlc": 
# {"open": 88.0, "high": 113.45, "low": 88.0, "close": 98.1}, 
# "change": 11.569826707441397, "last_trade_time": "2021-11-15T13:16:24", 
# "oi": 41625, "oi_day_high": 41625, "oi_day_low": 30500, "timestamp": 
# "2021-11-15T13:25:52", "depth": {"buy": [{"quantity": 500, "price": 105.05, "orders": 1}, 
# {"quantity": 500, "price": 104.75, "orders": 1}, {"quantity": 625, "price": 102.15, "orders": 1},
# {"quantity": 1000, "price": 97.65, "orders": 1}, {"quantity": 375, "price": 93.45, "orders": 1}], 
# "sell": [{"quantity": 125, "price": 108.15, "orders": 1}, {"quantity": 125, "price": 108.2, "orders": 1},
# {"quantity": 500, "price": 109.2, "orders": 1}, {"quantity": 125, "price": 110.0, "orders": 1}, {"quantity": 
# 125, "price": 110.05, "orders": 1}]}}}'''



# gcloud pubsub schemas validate-message \
#         --type=AVRO \
#         --definition= "{\
#   "name": "MyClass",\
#   "type": "array",\
#   "namespace": "com.acme.avro",\
#   "items": {\
#     "name": "MyClass_record",\
#     "type": "record",\
#     "fields": [\
#       {\
#         "name": "name",\
#         "type": "string"\
#       },\
#       {\
#         "name": "age",\
#         "type": "int"\
#       },\
#       {\
#         "name": "car",\
#         "type": "string"\
#       }\
#     ]\
#   }\
# }" \
#         --message-encoding=JSON \
#         --message="[{"name":"John", "age":30, "car":'a'}, {"name":"Mike", "age":28, "car":'b'}, {"name":"Adam", "age":33, "car":'b'}]"

# import json
# a= [{"name":"John", "age":30,"car":'a'},{"name":"Mike", "age":28, "car":'b'}, {"name":"Adam", "age":33, "car":'b'}]

# b = json.loads(a)
# print(b)

# [{"name":"John", "age":30, "car":'a'}]

# [{"tradable": true, "mode": "full", 
# "instrument_token": 20590082, "last_price": 152.0, "last_quantity": 
# 125, "average_price": 161.2, "volume": 13000, "buy_quantity": 4875, 
# "sell_quantity": 8000, "ohlc": {"open": 174.95, "high": 181.95, "low": 
# 146.2, "close": 176.45}, "change": -13.856616605270611, "last_trade_time": 
# "2021-11-15T13:24:03", "oi": 16125, "oi_day_high": 16250, "oi_day_low": 12125,
# "timestamp": "2021-11-15T13:25:52"}, 
#  {"tradable": true, "mode": "full", 
# "instrument_token": 20584194, "last_price": 109.45, "last_quantity": 125, "average_price": 106.58, 
# "volume": 17875, "buy_quantity": 6000, "sell_quantity": 12875, "ohlc": 
# {"open": 88.0, "high": 113.45, "low": 88.0, "close": 98.1}, 
# "change": 11.569826707441397, "last_trade_time": "2021-11-15T13:16:24", 
# "oi": 41625, "oi_day_high": 41625, "oi_day_low": 30500, "timestamp": 
# "2021-11-15T13:25:52"}]

# [{'tradable': False, 'mode': 'full', 'instrument_token': 260617, 'last_price': 18271.2, 
#   'ohlc': {'high': 18338.35, 'low': 18213.65, 'open': 18244.05, 'close': 18301.45}, 
#   'change': -0.16528744990151054, 'timestamp': "2021-11-15T13:25:52"}, 
#  {'tradable': False, 'mode': 'full', 'instrument_token': 264457, 'last_price': 9570.05, 'ohlc':
#      {'high': 9605.1, 'low': 9545.3, 'open': 9560.6, 'close': 9588.5},
#      'change': -0.19241800073004878, 'timestamp':"2021-11-15T13:25:52"}]

# list_req = ["instrument_token","last_price","ohlc","volume","buy_quantity", "sell_quantity","buy_quantity","timestamp","oi"]


# for k,v in d.items():
#     if k in list_req:
#         new_d[k] = v
#     else:
#         new_d = "null"

# {k:d[k] if k in d else k:1 for k in list_req}

# next = []
#     for tick in ticks:
#         new_d = {}
#         for k,v in tick.items():
#             if k in list_req:
#                 new_d[k] = v
#             else:
#                 new_d[k] = "null"    
#         next.append(new_d)
        
#   gcloud data-catalog entries update --lookup-entry='pubsub.topic.zerodha-332309.transactions' --schema-from-file=transactions_schema.yaml
# from json.decoder import JSONDecodeError
# import json
# input = ["prev_2.txt","prev_1.txt","prev_f.txt"]
# output = ["times_2.csv","times_1.csv","times_f.csv"]
# for i,j in zip(input,output):
# 	prev = {}
# 	with open(i, 'r') as infile:
# 			try:
# 				prev = json.load(infile)
# 			except JSONDecodeError:
# 				pass
			
# 	data = []

# 	for k, v in prev.items():
# 		data.append([v['instrument_token'], v['timestamp'],v['last_price'],v['volume']])

# 	import pandas as pd

# # 	data = pd.DataFrame(data)
# # 	data.to_csv(j)

# import glob
# path = r'Data/Final' # use your path
# all_files = glob.glob(path + "/*.csv")

# li = []
# import pandas as pd
# for filename in all_files:
#     df = pd.read_csv(filename, index_col=None, header=0)
#     li.append(df)

# frame = pd.concat(li, axis=0, ignore_index=True)
# frame.to_csv('all_token.csv')
# import os
# # fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","Data\Input"))

# gpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","Data\Output"))

# new = os.path.abspath(os.path.join(gpath,"final_list_one.csv"))

# import sys
# # print(fpath)
# print(new)

# import pandas as pd

# data = pd.read_csv(new)
# print(data.head())
# temp_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","temp_cache"))

# filename_data = os.path.abspath(os.path.join(temp_file_path,"prev_1.txt"))
# filename_timestamp = os.path.abspath(os.path.join(temp_file_path,"prevprev_timestamp_1_1.txt"))

# print(filename_data)

# # # from celery import Celery
# # # from time import sleep
# # # # import winsound

# # # app = Celery('tasks', broker = 'amqp://guest:guest@localhost:5672//', backend='db+sqlite:///db.sqlite3')

# # # from configparser import ConfigParser
# # # parser = ConfigParser()

# # # parser.read('test.ini')

# # # @app.task
# # # def add_print(x):
# # #     sleep(5)
# # #     parser.set('new', 'key', 'value')	
    
# # #     with open('test.ini', 'w') as configfile:
        
# # #         parser.write(configfile)
# # #     duration = 1000  # milliseconds
# # #     freq = 440  # Hz
# # #     winsound.Beep(freq, duration)
    
# # #     print(x*2)

# # # import pandas as pd
# # # indices_curr_list = pd.read_csv("index_currency_list.csv")

# # # indices_curr_list = indices_curr_list[indices_curr_list['name'] != 'USDINR']
# # # print(indices_curr_list)
# # # temp = indices_curr_list[indices_curr_list['period'] == 'Weekly']
# # # total_instruments = pd.read_csv("Data_Complete.csv")
# # # indexes = total_instruments[total_instruments['exchange'] == 'NSE']
# # # indexes = indexes[indexes['tradingsymbol'].isin(temp['symbol'])]

# # # print(indexes)

# # # import pandas as pd
# # # import datetime

# # # def expiry_days_symbol(months, weeks):
# # #     today = datetime.date.today()

# # #     month_symbol = []
# # #     for date in months:
# # #         if(date.date() < today):
# # #             continue
# # #         month_symbol.append(str(date.strftime('%y')).upper() + str(date.strftime('%b')).upper())
        
# # #     week_symbol = []

# #     for date in weeks:
# #         if(date.date() < today):
# #             continue
# #         week_symbol.append(str(date.strftime('%y')).upper() +
# #                         str(date.strftime('%b')).upper()[0] +
# #                         str(date.strftime('%d')).upper() )
# #      # Hard coded Data   
# #     return month_symbol[0:3], week_symbol[0:3]

# # index_expiry = pd.read_csv('Input_Data/expiry_index.csv')

# # index_weeks = index_expiry[index_expiry['Type'] == 'W']
# # index_weeks.loc[:,'Dates'] = pd.to_datetime(index_weeks.loc[:,'Dates'])
# # index_weeks = index_weeks['Dates'].tolist()


# # index_months = index_expiry[index_expiry['Type'] == 'M']
# # index_months.loc[:,'Dates'] = pd.to_datetime(index_months.loc[:,'Dates'])
# # index_months = index_months['Dates'].tolist()

# # # print(weeks, months)
# # month_symbol_index, week_symbol_index = expiry_days_symbol(index_months,index_weeks)
# # print(month_symbol_index, week_symbol_index)


# # # Insert all the instruments in database
# # import pandas as pd
# # import glob
# # from config import config
# # import psycopg2

# # path = r'G:\DS - Competitions and projects\Zerodha\Data\\Final' # use your path
# # all_files = glob.glob(path + "/*.csv")

# # li = []

# # for filename in all_files:
# #     df = pd.read_csv(filename, index_col=None, header=0)
# #     li.append(df)

# # frame = pd.concat(li, axis=0, ignore_index=True)

# # print(frame.shape[0])
# # print(frame.head())
# # params = config()

# # user = params['user']
# # password = params['password']     
# # print(user, password) 
# # from sqlalchemy import create_engine
# # engine = create_engine(f'postgresql://{user}:{password}@localhost:5432/test')
# # frame.to_sql('instrument_list', engine, if_exists= 'replace')

# import json
# # d = "fgfgsgs"
# # json.dump(d, open("try.txt",'w'))
# # # d2 = json.load(open("try.txt"))
# # # print(d2)

# # # d2 = {}
# # from json.decoder import JSONDecodeError

# # with open("trgfy.txt", 'w+') as infile:
# #     try:
# #         d2 = json.load(infile)
# #         # data = old_data + obj
# #         # json.dump(data, outfile)
# #     except JSONDecodeError:
# #         pass

# # print(d2)

# # a = "2021-11-11 12:41:24"

# # import datetime
# # next_timestamp = datetime.datetime.strptime(a,'%Y-%m-%d %H:%M:%S')
# # # print(a)
# # open('prev1212.txt', 'w').close()
# # open('prev_timestamp212.txt', 'w').close()

# # import glob
# # path = r'Data/Final' # use your path
# # all_files = glob.glob(path + "/*.csv")

# # print(len(all_files),all_files)

# # mod_files = []
# # for file in all_files:
# #     if 'stock' not in file:
# #        mod_files.append(file)

# # print(len(mod_files),mod_files) 

# # import pandas as pd
# # li = []
# # for filename in mod_files:
# #     df = pd.read_csv(filename, index_col=None, header=0)
# #     li.append(df)

# # frame = pd.concat(li, axis=0, ignore_index=True)

# # print(frame.head())
# # instruments = frame['instrument_token'].tolist()
# # print(len(instruments))
# # # with open("prev.txt", 'w') as outfile:
# #         try:
# #             json.dump(None, outfile)

# #         except JSONDecodeError:
# #             pass
# # with open("prev_timestamp.txt", 'w') as outfile:
# #         try:
# #              json.dump(None, outfile)
# #         except JSONDecodeError:
# #             pass

# # text = '[{"tr_time_str": "2021-11-17 19:00:40", "first_name": "Stephane", "last_name": "Ulrike", "city": "Arlington", "state": "CT", "product": "Product 5 XL", "amount": 687.85},{"tr_time_str": "2021-11-17 19:00:40", "first_name": "John", "last_name": "Doe", "city": "Arlington", "state": "CT", "product": "Product 5 XL", "amount": 687.85}]'
# # a = json.dumps(text)
# # print(type(a))

# # import os
# # import datetime



# # now = datetime.datetime.now()
# # date_time = now.strftime("%m-%d-%Y-%H-%M-%S")

# # # Create a cloud Dataflow job
# # job_name = "dfsql-hil8s363-kwdgvpuh" + date_time

# # gc_command = f'gcloud dataflow sql query "SELECT tr.* FROM pubsub.topic.`zerodha-332309`.ticker_data  as tr" '\
# # f'--job-name {job_name} ' \
# # '--region asia-south1 '\
# # '--bigquery-write-disposition write-empty '\
# # '--bigquery-project zerodha-332309 '\
# # '--bigquery-dataset ticker_data --bigquery-table new_day_1'

# # os.system(gc_command)
# import threading
# import os
# class myThread (threading.Thread):
# 	def __init__(self, command):
# 		threading.Thread.__init__(self)
# 		self.cmd = command

# 	def run(self):
# 		print ("Starting " + self.cmd)
# 		os.system(self.cmd)
# 		print ("Exiting " + self.cmd)

# # Commands to run parallely
# lstCmd=['cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_f  worker -Q fando --concurrency=1  --loglevel=info -P  eventlet -n worker1@%h', 
# 		'cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_1  worker -Q stock1 --concurrency=1  --loglevel=info -P  eventlet -n worker2@%h',
# 		'cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_2  worker -Q stock2 --concurrency=1  --loglevel=info -P  eventlet -n worker3@%h'
# 		]   

# # Create new threads
# thread1 = myThread(lstCmd[0])
# thread2 = myThread(lstCmd[1])
# thread3 = myThread(lstCmd[2])
# # thread4 = myThread(lstCmd[3])
# # thread5 = myThread(lstCmd[4])
# # thread6 = myThread(lstCmd[5])


# # Start new Threads
# thread1.start()
# thread2.start()
# thread3.start()
# # thread4.start()
# # thread5.start()
# # thread6.start()

# from tqdm import tqdm
# import time
# import time
# for i in tqdm(range(300)):
#     time.sleep(1)

# import os
# a = os.path.abspath(os.path.join(os.path.dirname(__file__)))
# print(a)
# import threading

# # Class mythread inheriting thread class
# class myThread (threading.Thread):
#     def __init__(self, command):
#         threading.Thread.__init__(self)
#         self.cmd = command

#     def run(self):
#         print ("Starting " + self.cmd)
#         os.system(self.cmd)
#         print ("Exiting " + self.cmd)

# worker = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..", "src/publish_database"))

# # Commands to run parallely
# lstCmd=['cd "' + worker +'" && celery -A publish_pubsub_1 worker -Q conn_1 --concurrency=1  --loglevel=info -P  eventlet -n worker1@%h']

# thread1 = myThread(lstCmd[0])

# thread1.start()        
# import datetime
# now = datetime.datetime.now()
# date_time = now.strftime("%m-%d-%Y-%H-%M-%S")

# table_name = 'new_day_'+str(now.day)
# print(table_name)
# import os

# rabbitmq_del_queue = './rabbitmqadmin -f tsv -q list queues name | while read queue; do ./rabbitmqadmin -q delete queue name=${queue}; done'
# print(rabbitmq_del_queue)
# os.system(rabbitmq_del_queue)


# gcloud dataflow sql query "SELECT tr.* FROM pubsub.topic.\\\`zerodha-332309\\\`.ticker_data_new as tr" --job-name dasdasdasd  --region asia-south1 --bigquery-write-disposition write-empty --bigquery-project zerodha-332309 
# --bigquery-dataset ticker_data --bigquery-table new_Tt


# import os
# gcloud_sdk_path = '/home/tariqanwarph/Downloads/google-cloud-sdk/bin/'
# filename_job = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..","job.txt"))

#     # Write job name to file
# with open(filename_job, 'r') as infile:
#     job_name = infile.read()

# gc_command = f'{gcloud_sdk_path}gcloud dataflow jobs cancel '\
# f'{job_name} ' \
# '--region asia-south1 '
# # os.system(gc_command)
# import threading
# import os
# import yaml

# # Reading config from yaml file
# config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
# try: 
#     with open (config_path, 'r') as file:
#     	config = yaml.safe_load(file)
# except Exception as e:
#     print('Error reading the config file')
# # Class mythread inheriting thread class
# class myThread (threading.Thread):
#     def __init__(self, command):
#         threading.Thread.__init__(self)
#         self.cmd = command

#     def run(self):
#         print ("Starting " + self.cmd)
#         os.system(self.cmd)
#         print ("Exiting " + self.cmd)

# worker = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "publish_database"))
# ticker_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..", "ticker"))

# queue_1 = config['rabbitmq']['queues']['one']
# queue_2 = config['rabbitmq']['queues']['two']
# queue_3 = config['rabbitmq']['queues']['three']
# queue_4 = config['rabbitmq']['queues']['four']
# queue_5 = config['rabbitmq']['queues']['five']
# queue_6 = config['rabbitmq']['queues']['six']
# # Commands to run parallely
# lstCmd=['cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_1} --concurrency=1  --loglevel=info -P  eventlet -n worker1@%h', 
#         'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_2} --concurrency=1  --loglevel=info -P  eventlet -n worker2@%h',
#         'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_3} --concurrency=1  --loglevel=info -P  eventlet -n worker3@%h',
#         'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_4} --concurrency=1  --loglevel=info -P  eventlet -n worker4@%h', 
#         'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_5} --concurrency=1  --loglevel=info -P  eventlet -n worker5@%h',
#         'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_6} --concurrency=1  --loglevel=info -P  eventlet -n worker6@%h',
#         'cd "' + ticker_path +'" && python save_data_conn1.py',
#         'cd "' + ticker_path +'" && python save_data_conn2.py',
#         'cd "' + ticker_path +'" && python save_data_conn3.py',
#         'cd "' + ticker_path +'" && python save_data_conn4.py',
#         'cd "' + ticker_path +'" && python save_data_conn5.py',
#         'cd "' + ticker_path +'" && python save_data_conn6.py'
#         ]   

# # Create new threads
# thread1 = myThread(lstCmd[0])
# thread2 = myThread(lstCmd[1])
# thread3 = myThread(lstCmd[2])
# thread4 = myThread(lstCmd[3])
# thread5 = myThread(lstCmd[4])
# thread6 = myThread(lstCmd[5])
# thread7 = myThread(lstCmd[6])
# thread8 = myThread(lstCmd[7])
# thread9 = myThread(lstCmd[8])
# thread10 = myThread(lstCmd[9])
# thread11 = myThread(lstCmd[10])
# thread12 = myThread(lstCmd[11])

# threads = [thread1,thread2,thread3,thread4,thread5,
#             thread6,thread7,thread8,thread9,thread10,
#             thread11,thread12]

# # Start new Threads
# thread1.start()
# thread2.start()
# thread3.start()
# thread4.start()
# thread5.start()
# thread6.start()
# # thread7.start()
# # thread8.start()
# # thread9.start()
# # thread10.start()
# # thread11.start()
# # thread12.start()
import os
import yaml
import glob
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
        config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')

main_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..",config['crosscheck_file_location']))
all_files = glob.glob(main_path + "/*.csv")
for file in all_files:
    attachment = open(file, "rb")
    filename = file.split('/')[-1]
    print(filename)