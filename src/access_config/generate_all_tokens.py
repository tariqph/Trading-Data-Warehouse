import numpy as np
import pandas as pd
from access_token import access_token
import math
import datetime
from forex_python.converter import CurrencyRates
import glob
from config import config
from sqlalchemy import create_engine
import os

# Input and Output folders
input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","Data\Input"))
output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","Data\Output"))


def generate_tokens(total_instruments,instruments,filename,exchange):
    '''Generates tokens from the trading symbols using the list downloaded
    from kite everyday and saves it in a csv file 
    '''
    all1_instruments = total_instruments[total_instruments['exchange'] == exchange]
    all1_instruments = all1_instruments[all1_instruments['tradingsymbol'].isin(instruments['tradingsymbol'])]
    all1_instruments = all1_instruments[['instrument_token','name','tradingsymbol','strike','expiry','instrument_type']]

    all1_instruments.to_csv(filename)
    

def expiry_days_symbol(months, weeks):
    ''' Generates the symbols for monthly and weekly expiry dates to be concatenated with instruments to get
    the tradingsymbol'''
    today = datetime.date.today()
    month_symbol = []
    for date in months:
        if(date.date() < today):
            continue
        month_symbol.append(str(date.strftime('%y')).upper() + str(date.strftime('%b')).upper())
        
    week_symbol = []

    for date in weeks:
        if(date.date() < today):
            continue
        week_symbol.append(str(date.strftime('%y')).upper() +
                        str(date.strftime('%b')).upper()[0] +
                        str(date.strftime('%d')).upper() )
     # Hard coded Data   
    return month_symbol[0:3], week_symbol[0:3]

def modify(num):
    ''' modifies the strike price to the required number of decimals to be concatenated with instruments
    list to generate trading symbols'''
    if(num%1==0):
        return math.trunc(num)
    elif(num%0.25==0):
        return round(num,2)
    else:
        return round(num,1)

    
def add_columns(all_instruments,key):
    ''' Adds columns to the instruments dataframe.The columns are used to construct the range 
    of strikes that needs to be downloaded '''   
    if(key == 'stocks'):
        all_instruments.loc[:,'ltp_plus'] = all_instruments.apply(lambda x: x['ltp']*(1+x['threshold']), axis=1)
        all_instruments.loc[:,'ltp_minus'] = all_instruments.apply(lambda x: x['ltp']*(1-x['threshold']), axis=1)
    else:
        all_instruments.loc[:,'ltp_plus'] = all_instruments.apply(lambda x: (x['ltp']+ x['threshold']), axis=1)
        all_instruments.loc[:,'ltp_minus'] = all_instruments.apply(lambda x: (x['ltp']-x['threshold']), axis=1)
  

    all_instruments.loc[:,'start'] = all_instruments.apply(
        lambda x: x['ltp_minus'] - (x['ltp_minus']%x['pip']), axis=1)

    all_instruments.loc[:,'stop'] = all_instruments.apply(
        lambda x: x['ltp_plus'] - (x['ltp_plus']%x['pip']), axis=1)

    all_instruments.loc[:,'steps'] = all_instruments.apply(
        lambda x: (x['stop'] - x['start'])/x['pip'] + 1, axis=1)
    
    return all_instruments

def generate_list(instruments,key, filename, symbol, option_type):
    ''' Generates a list of of trading symbols for the options with different strikes, expiry and type options.'''
    
    if(key == 'index'):
        a = instruments['name_y'].to_list()
    elif(key == 'stock'):
        a = instruments['tradingsymbol'].to_list()
    else:
        a = instruments['symbol'].to_list()
    
    # if(key =='curr'):
    #     # print(instruments)    
        
    b = instruments['start'].to_list()
    c = instruments['stop'].to_list()
    d = instruments['steps'].to_list()

    # print(month_symbol,week_symbol)

    final = []
    for i in range(len(a)):
        w = np.linspace(int(b[i]),int(c[i]),int(d[i]))
        z = [n+l+str(modify(k))+o for n in [a[i]] for l in symbol for k in w for o in option_type]
        final = final + z

    final = pd.DataFrame(final, columns = ['tradingsymbol'])
    final.to_csv(filename)

    
kite = access_token()


# Input Files location

# stock_list_input = "Input_Data/stock_list.csv"
stock_list_input = os.path.abspath(os.path.join(input_path,"stock_list.csv")) 

# indices_list_input = "Input_Data/index_currency_list.csv"
indices_list_input = os.path.abspath(os.path.join(input_path,"index_currency_list.csv")) 

# total_instruments_input = "Input_Data/Data_Complete.csv"
total_instruments_input = os.path.abspath(os.path.join(input_path,"Data_Complete.csv")) 

# index_expiry_input  = 'Input_Data/expiry_index.csv'
index_expiry_input = os.path.abspath(os.path.join(input_path,"expiry_index.csv")) 

# curr_expiry_input = 'Input_Data/expiry_curr.csv'
curr_expiry_input = os.path.abspath(os.path.join(input_path,"expiry_curr.csv")) 


# Get the current list of instruments from zerodha
data = kite.instruments()
data = pd.DataFrame(data)
data.to_csv(total_instruments_input)

stock_list = pd.read_csv(stock_list_input)
indices_curr_list = pd.read_csv(indices_list_input)

indices_list = indices_curr_list[indices_curr_list['name'] != 'USDINR'].copy()
curr_list = indices_curr_list[indices_curr_list['name'] == 'USDINR'].copy()

# LTP of USDINR
c = CurrencyRates()
curr_list.loc[:,'ltp'] = round(c.get_rate('USD', 'INR'),2) 
# curr_list.loc[:,'ltp'] = 75 
print(curr_list)


# From the symbols get the tokens to call kite for the LTPs
total_instruments = pd.read_csv(total_instruments_input)
all_instruments = total_instruments[total_instruments['exchange'] == 'NSE'].copy()
all_instruments = all_instruments[all_instruments['tradingsymbol'].isin(stock_list['symbol'])].copy()

temp = indices_list[indices_list['period'] == 'Weekly'].copy()
indexes = total_instruments[total_instruments['exchange'] == 'NSE'].copy()
indexes = indexes[indexes['tradingsymbol'].isin(temp['symbol'])].copy()

all_instruments_tokens = all_instruments.instrument_token.to_numpy()
indexes_tokens = indexes.instrument_token.to_numpy()

# Getting LTP of all the instruments
ltp = kite.ltp(all_instruments_tokens)
ltp_indexes = kite.ltp(indexes_tokens)

ltps = [[]]
for key, value in ltp.items():
    ltps.append([value['instrument_token'], value['last_price']])
    
ltps = pd.DataFrame(ltps, columns=['instrument_token','ltp'])

ltps_indexes = [[]]

for key, value in ltp_indexes.items():
    ltps_indexes.append([value['instrument_token'], value['last_price']])
    
ltps_indexes = pd.DataFrame(ltps_indexes, columns=['instrument_token','ltp'])

all_instruments=all_instruments.merge(ltps, on='instrument_token', how='left')
all_instruments = all_instruments.merge(stock_list, left_on='tradingsymbol',
                                                        right_on='symbol', how='left')

indexes = indexes.merge(ltps_indexes, on='instrument_token', how='left')
indexes = indexes.merge(indices_list, left_on='tradingsymbol',
                                                        right_on='symbol', how='left')

# Adding additional columns to dataframe for the range of strikes
all_instruments = add_columns(all_instruments,'stocks')
all_instruments_indexes = add_columns(indexes,'index')
all_instruments_curr = add_columns(curr_list,'curr')

three_month_instruments = all_instruments[all_instruments['months_req'] == 3].copy()
one_month_instruments = all_instruments[all_instruments['months_req'] == 1].copy()

weekly_index = indexes[indexes['period'] == 'Weekly'].copy()
monthly_index = indexes[indexes['period'] == 'Monthly'].copy()

weekly_curr = curr_list[curr_list['period'] == 'Weekly'].copy()
monthly_curr = curr_list[curr_list['period'] == 'Monthly'].copy()

option_type = ['CE', 'PE']

# Generate symbols for expiry for index and currency from the list of dates in a file
index_expiry = pd.read_csv(index_expiry_input)

index_weeks = index_expiry[index_expiry['Type'] == 'W'].copy()
index_weeks.loc[:,'Dates'] = pd.to_datetime(index_weeks.loc[:,'Dates'])
index_weeks = index_weeks['Dates'].tolist()


index_months = index_expiry[index_expiry['Type'] == 'M'].copy()
index_months.loc[:,'Dates'] = pd.to_datetime(index_months.loc[:,'Dates'])
index_months = index_months['Dates'].tolist()

month_symbol_index, week_symbol_index = expiry_days_symbol(index_months,index_weeks)

curr_expiry = pd.read_csv(curr_expiry_input)

curr_weeks = curr_expiry[curr_expiry['Type'] == 'W'].copy()
curr_weeks.loc[:,'Dates'] = pd.to_datetime(curr_weeks.loc[:,'Dates'])
curr_weeks = curr_weeks['Dates'].tolist()

curr_months = curr_expiry[curr_expiry['Type'] == 'M'].copy()
curr_months.loc[:,'Dates'] = pd.to_datetime(curr_months.loc[:,'Dates'])
curr_months = curr_months['Dates'].tolist()

# Getting expiry symbols
month_symbol_curr, week_symbol_curr = expiry_days_symbol(curr_months,curr_weeks)

# Output location of trading symbol list

# three_mon_output = 'Data/final_list_three.csv'
three_mon_output = os.path.abspath(os.path.join(output_path,"final_list_three.csv")) 

# one_mon_output = 'Data/final_list_one.csv'
one_mon_output = os.path.abspath(os.path.join(output_path,"final_list_one.csv")) 

# weekly_index_output = 'Data/weekly_index.csv'
weekly_index_output = os.path.abspath(os.path.join(output_path,"weekly_index.csv")) 

# monthly_index_output = 'Data/monthly_index.csv'
monthly_index_output = os.path.abspath(os.path.join(output_path,"monthly_index.csv")) 

# weekly_curr_output = 'Data/weekly_curr.csv'
weekly_curr_output = os.path.abspath(os.path.join(output_path,"weekly_curr.csv")) 

# monthly_curr_output = 'Data/monthly_curr.csv'
monthly_curr_output = os.path.abspath(os.path.join(output_path,"monthly_curr.csv")) 


generate_list(three_month_instruments,'stock',three_mon_output,month_symbol_index[0:2],option_type)
generate_list(one_month_instruments,'stock',one_mon_output,[month_symbol_index[0]],option_type)
generate_list(weekly_index,'index',weekly_index_output,week_symbol_index,option_type)
generate_list(monthly_index,'index',monthly_index_output,month_symbol_index,option_type)
generate_list(weekly_curr,'curr',weekly_curr_output,week_symbol_curr,option_type)
generate_list(monthly_curr,'curr',monthly_curr_output,month_symbol_curr,option_type)

index_weekly = pd.read_csv(weekly_index_output)
index_monthly = pd.read_csv(monthly_index_output)

curr_weekly = pd.read_csv(weekly_curr_output)
curr_monthly = pd.read_csv(monthly_curr_output)

stock_two_month = pd.read_csv(three_mon_output)
stock_one_month = pd.read_csv(one_mon_output)

# Output location of the final token list for total_instrument

# final_token_index_weekly = 'Data/Final/final_token_index_weekly.csv'
final_token_index_weekly = os.path.abspath(os.path.join(output_path,"Final/final_token_index_weekly.csv")) 

# final_token_index_monthly = 'Data/Final/final_token_index_monthly.csv'
final_token_index_monthly = os.path.abspath(os.path.join(output_path,"Final/final_token_index_monthly.csv")) 

# final_token_stock_three_month = 'Data/Final/final_token_stock_three_month.csv'
final_token_stock_three_month = os.path.abspath(os.path.join(output_path,"Final/final_token_stock_three_month.csv")) 

# final_token_stock_one_month = 'Data/Final/final_token_stock_one_month.csv'
final_token_stock_one_month = os.path.abspath(os.path.join(output_path,"Final/final_token_stock_one_month.csv")) 

# final_token_curr_weekly = 'Data/Final/final_token_curr_weekly.csv'
final_token_curr_weekly = os.path.abspath(os.path.join(output_path,"Final/final_token_curr_weekly.csv")) 

# final_token_curr_monthly = 'Data/Final/final_token_curr_monthly.csv'
final_token_curr_monthly = os.path.abspath(os.path.join(output_path,"Final/final_token_curr_monthly.csv")) 

# final_curr_futures_token = 'Data/Final/final_curr_futures_token.csv'
final_curr_futures_token = os.path.abspath(os.path.join(output_path,"Final/final_curr_futures_token.csv")) 

# final_index_futures_token = 'Data/Final/final_index_futures_token.csv'
final_index_futures_token = os.path.abspath(os.path.join(output_path,"Final/final_index_futures_token.csv")) 

# final_token_indices_list_NSE = 'Data/Final/final_token_indices_list_NSE.csv'
final_token_indices_list_NSE = os.path.abspath(os.path.join(output_path,"Final/final_token_indices_list_NSE.csv")) 

# final_token_indices_list_BSE = 'Data/Final/final_token_indices_list_BSE.csv'
final_token_indices_list_BSE = os.path.abspath(os.path.join(output_path,"Final/final_token_indices_list_BSE.csv")) 



generate_tokens(total_instruments,index_weekly,final_token_index_weekly,'NFO')
generate_tokens(total_instruments,index_monthly,final_token_index_monthly,'NFO')

generate_tokens(total_instruments,stock_two_month,final_token_stock_three_month,'NFO')
generate_tokens(total_instruments,stock_one_month,final_token_stock_one_month,'NFO')

generate_tokens(total_instruments,curr_weekly,final_token_curr_weekly,'CDS')
generate_tokens(total_instruments,curr_monthly,final_token_curr_monthly,'CDS')


# Generating the tokens for Futures
curr_futures = total_instruments[(total_instruments['exchange'] == 'CDS') & 
                                 (total_instruments['instrument_type'] == 'FUT') 
                                 ].copy()
curr_futures = curr_futures[curr_futures['name'].isin(['USDINR','EURINR','JPYINR','GBPINR'])].copy()

curr_futures = curr_futures[['instrument_token','name','tradingsymbol','strike','expiry','instrument_type']].copy()
curr_futures.to_csv(final_curr_futures_token)

index_futures = total_instruments[(total_instruments['exchange'] == 'NFO') & 
                                 (total_instruments['instrument_type'] == 'FUT')].copy()
index_futures = index_futures[['instrument_token','name','tradingsymbol','strike','expiry','instrument_type']].copy()
index_futures.to_csv(final_index_futures_token)

# Generating the tokens for Indices
indices_path = os.path.abspath(os.path.join(input_path,"Indexes.csv")) 

indices_list = pd.read_csv(indices_path)
generate_tokens(total_instruments, indices_list,final_token_indices_list_NSE,'NSE')
generate_tokens(total_instruments, indices_list,final_token_indices_list_BSE,'BSE')



# Insert the final instrument list with tokens generated into a database table

# path = r'Data/Final' # use your path
path = os.path.abspath(os.path.join(output_path,"Final")) 

all_files = glob.glob(path + "/*.csv")

li = []

for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)

params = config()

user = params['user']
password = params['password']     

engine = create_engine(f'postgresql://{user}:{password}@localhost:5432/test_final')
frame.to_sql('instruments_list_today', engine, if_exists= 'replace')
frame.to_csv(path + '_all_instruments_today.csv')