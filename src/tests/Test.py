from google.cloud import bigquery
import os
import datetime
import yaml

import pandas as pd
from random import sample

def gen_crosscheck_files():
    '''
    Genrates 10 or less random crosscheck instruments to be sent along with the logs
    '''
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 
                                                "..","..","config.yml"))
    try: 
        with open (config_path, 'r') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        print('Error reading the config file')
        
    key_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 
                                            "..","..",config['gcloud']['bigquery_key']))

    # client = pubsub_v1.PublisherClient.from_service_account_json(key_path,batch_settings)
    now = datetime.datetime.now()

    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), 
                                            "..","..",config['output_files']['merged_tokens']['all_instruments']))

    all_instruments = pd.read_csv(filepath)
    instrument_tokens = all_instruments['instrument_token'].tolist()
    trading_symbols = all_instruments['tradingsymbol'].tolist()
    merged_list = list(zip(instrument_tokens,trading_symbols))
    sample_tokens = sample(merged_list,10)

    for token, symbol in sample_tokens:
        today_date = datetime.datetime.today()
        day_num = today_date.day 
        
        query  = f'''SELECT exchange_timestamp, last_price FROM `zerodha-332309.ticker_data.new_day_{day_num}` 
        WHERE instrument_token = {token} order by exchange_timestamp asc
        '''
    
        client = bigquery.Client.from_service_account_json(key_path)
        query_job = client.query(query)
        result = query_job.result()

        data = []
        for row in result:
            data.append([token,symbol,row.exchange_timestamp, row.last_price])
            # print(row.exchange_timestamp, row.last_price)
        if(len(data) > 0):
            data =  pd.DataFrame(data)
            data.columns = ['instrument_token','tradingsymbol','exchange_timestamp','last_price']
            filepath_csv = os.path.abspath(os.path.join(os.path.dirname(__file__), 
                                                    "..","..",config['crosscheck_file_location'],f"{symbol}.csv"))
            data.to_csv(filepath_csv)
            
