from google.cloud import bigquery
import os
import datetime
import yaml

def bq_insert():
    
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
    try: 
        with open (config_path, 'r') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        print('Error reading the config file')
        
    key_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",config['gcloud']['bigquery_key']))

    # client = pubsub_v1.PublisherClient.from_service_account_json(key_path,batch_settings)
    now = datetime.datetime.now()

    PROJECT_ID = config['gcloud']['project_name']
    table_id = "instrument_list_" + str(now.day)
    dataset_id = config['gcloud']['dataset_id']

    client = bigquery.Client.from_service_account_json(key_path)


    # some variables
    filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                            config['output_files']['merged_tokens']['all_instruments']))
    # filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
    #                                         "Data/Output/Final_all_instruments_today.csv"))
    try:
        client.create_table(f"{PROJECT_ID}.{dataset_id}.{table_id}")   
    except Exception as e:
            print(e)
            return
    
    # tell the client everything it needs to know to upload our csv
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True

    # load the csv into bigquery
    with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
            
    print(job.result()) 