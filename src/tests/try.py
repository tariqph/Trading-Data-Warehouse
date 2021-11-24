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
import os
# fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","Data\Input"))

gpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","Data\Output"))

new = os.path.abspath(os.path.join(gpath,"final_list_one.csv"))

import sys
# print(fpath)
print(new)

import pandas as pd

data = pd.read_csv(new)
print(data.head())
temp_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","temp_cache"))

filename_data = os.path.abspath(os.path.join(temp_file_path,"prev_1.txt"))
filename_timestamp = os.path.abspath(os.path.join(temp_file_path,"prevprev_timestamp_1_1.txt"))

print(filename_data)