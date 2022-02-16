import os
import threading
import datetime 
from json.decoder import JSONDecodeError
import time
from tqdm import tqdm
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import datetime 
import glob
from configparser import ConfigParser
import yaml
import sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__),"tests"))
sys.path.append(fpath)

from Test import gen_crosscheck_files
# Reading config from yaml file


def email_logs():
    # Reading config from file
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","config.yml"))
    try: 
        with open (config_path, 'r') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        print('Error reading the config file')

    main_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",config['crosscheck_file_location']))
    all_files = glob.glob(main_path + "/*.csv")
    fromaddr = config['email']['sender']
    toaddr = config['email']['reciever']
    
    # instance of MIMEMultipart
    msg = MIMEMultipart()
    
    msg['From'] = fromaddr    
    msg['To'] = toaddr

    date_today = datetime.date.today()
    # storing the subject 
    msg['Subject'] = f"Log file : {date_today}"
    
    body = "Attached is the log file for today's"
    
    msg.attach(MIMEText(body, 'plain'))
    
    # open the file to be sent 
    filename = "run.log"
    log_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",f"logs/run_{date}.log"))

    attachment = open(log_file, "rb")
    
    # instance of MIMEBase and named as p
    p = MIMEBase('application', 'octet-stream')
    
    # To change the payload into encoded form
    p.set_payload((attachment).read())
    
    # encode into base64
    encoders.encode_base64(p)
    
    p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    
    # attach the instance 'p' to instance 'msg'
    msg.attach(p)

    for file in all_files:
        attachment = open(file, "rb")
        filename = file.split('/')[-1]
        # instance of MIMEBase and named as p
        p = MIMEBase('application', 'octet-stream')

        # To change the payload into encoded form
        p.set_payload((attachment).read())

        # encode into base64
        encoders.encode_base64(p)

        p.add_header('Content-Disposition', "attachment; filename= %s" % filename)

        # attach the instance 'p' to instance 'msg'
        msg.attach(p)
        attachment.close()
        os.remove(file)
        
    # creates SMTP session
    s = smtplib.SMTP('smtp.gmail.com', 587)
    
    # start TLS for security
    s.starttls()
    
    # read config file
    filename_password = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                          "..",config['access_files']['api_one']))
    parser = ConfigParser()
    parser.read(filename_password)

    db = {}
    section = 'email_cred'
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    # Authentication
    s.login(fromaddr, db['password'])
    
    # Converts the Multipart msg into a string
    text = msg.as_string()
    
    # sending the mail
    s.sendmail(fromaddr, toaddr, text)
    
    # terminating the session
    s.quit()


def dataflow_job():
    now = datetime.datetime.now()
    date_time = now.strftime("%m-%d-%Y-%H-%M-%S")
    filename_job = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","job.txt"))
    
    logging.info('Dataflow Job started at%s',date_time)

    # Create a cloud Dataflow job
    job_name = "dfsql-hil8s363-kwdgvpuh" + date_time
    table_name = 'new_day_' + str(now.day)
    gcloud_sdk_path = '/home/tariqanwarph/Downloads/google-cloud-sdk/bin/'
    
    gc_command = f'{gcloud_sdk_path}gcloud dataflow sql query "SELECT tr.* FROM pubsub.topic.\`zerodha-332309\`.ticker_data_new as tr" '\
    f'--job-name {job_name} ' \
    '--region us-central1 '\
    '--bigquery-write-disposition write-empty '\
    '--bigquery-project zerodha-332309 '\
    f'--bigquery-dataset ticker_data --bigquery-table {table_name} --max-workers 1 --worker-machine-type n1-standard-1'

    a = os.popen(gc_command).read()
    s = a.replace('\n',':')
    s = s.split(':')
    s = [i.strip() for i in s]
    with open(filename_job, 'w') as outfile:
        for i in range(len(s)):
            if(s[i] == 'id'):
                outfile.write(s[i+1])
                
    logging.info("Wait for the Dataflow job to start")
    # Sleep for a few minutes to let the Dataflow job start
    for i in tqdm(range(120)):
        time.sleep(1)
    logging.info("proceeding")

def gen_tokens():
    
# Generating Access Tokens
    logging.info("Generating day account access and instrument tokens")
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "access_config"))

    os.system('cd "'+ path +'" && python generate_access_token.py')
    # # Generating instrument tokens
    print("Generating Instrument tokens")
    os.system('cd "'+ path +'" && python generate_all_tokens.py')


def celery_websocket():

    # Reading config from file
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","config.yml"))
    try: 
        with open (config_path, 'r') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        print('Error reading the config file')

    logging.info('Deleting queues from rabbitmq')
    rabbitmq_del_queue = './rabbitmqadmin -f tsv -q list queues name | while read queue; do ./rabbitmqadmin -q delete queue name=${queue}; done'
    os.system(rabbitmq_del_queue)

    logging.info("Collecting Data")
# Class mythread inheriting thread class
    class myThread (threading.Thread):
        def __init__(self, command):
            threading.Thread.__init__(self)
            self.cmd = command

        def run(self):
            print ("Starting " + self.cmd)
            os.system(self.cmd)
            print ("Exiting " + self.cmd)

    worker = os.path.abspath(os.path.join(os.path.dirname(__file__), "publish_database"))
    ticker_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "ticker"))
    
    queue_1 = config['rabbitmq']['queues']['one']
    queue_2 = config['rabbitmq']['queues']['two']
    queue_3 = config['rabbitmq']['queues']['three']
    queue_4 = config['rabbitmq']['queues']['four']
    queue_5 = config['rabbitmq']['queues']['five']
    queue_6 = config['rabbitmq']['queues']['six']

    # Commands to run parallely
    lstCmd=['cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_1} --concurrency=1  --loglevel=info -P  eventlet -n worker1@%h', 
            'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_2} --concurrency=1  --loglevel=info -P  eventlet -n worker2@%h',
        'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_3} --concurrency=1  --loglevel=info -P  eventlet -n worker3@%h',
            'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_4} --concurrency=1  --loglevel=info -P  eventlet -n worker4@%h', 
            'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_5} --concurrency=1  --loglevel=info -P  eventlet -n worker5@%h',
            'cd "' + worker +f'" && celery -A publish_pubsub_1  worker -Q {queue_6} --concurrency=1  --loglevel=info -P  eventlet -n worker6@%h',
            'cd "' + ticker_path +'" && python save_data_conn1.py',
            'cd "' + ticker_path +'" && python save_data_conn2.py',
            'cd "' + ticker_path +'" && python save_data_conn3.py',
            'cd "' + ticker_path +'" && python save_data_conn4.py',
            'cd "' + ticker_path +'" && python save_data_conn5.py',
            'cd "' + ticker_path +'" && python save_data_conn6.py'
            ]   

    # Create new threads
    thread1 = myThread(lstCmd[0])
    thread2 = myThread(lstCmd[1])
    thread3 = myThread(lstCmd[2])
    thread4 = myThread(lstCmd[3])
    thread5 = myThread(lstCmd[4])
    thread6 = myThread(lstCmd[5])
    thread7 = myThread(lstCmd[6])
    thread8 = myThread(lstCmd[7])
    thread9 = myThread(lstCmd[8])
    thread10 = myThread(lstCmd[9])
    thread11 = myThread(lstCmd[10])
    thread12 = myThread(lstCmd[11])

    threads = [thread1,thread2,thread3,thread4,thread5,
               thread6,thread7,thread8,thread9,thread10,
               thread11,thread12]

    # Start new Threads
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()
    thread7.start()
    thread8.start()
    thread9.start()
    thread10.start()
    thread11.start()
    thread12.start()

    # exit_time_curr = "17:00:00"
    # exit_time_index = "15:30:00"

    # exit_time_curr = datetime.datetime.strptime(exit_time_curr,"%H:%M:%S").time()
    # exit_time_index = datetime.datetime.strptime(exit_time_index,"%H:%M:%S").time()

     
    while True:
        time.sleep(30)
        
        time_now = datetime.datetime.now()
        for i in range(len(threads)):
            if threads[i].is_alive():
                if(time_now.minute % 30 == 0):
                    logging.info('Time:%s',time_now)
                    logging.info('Thread alive: %s',lstCmd[i][-26:])
                continue
                
            else:
                # The currency data is collected using conn_1 so it runs till 17:00
                if(i <= 6 and time_now.hour < 17 ):

                    logging.error('Time:%s',time_now)
                    logging.error('Thread died: %s',lstCmd[i][-26:])
                    new_thread = myThread(lstCmd[i])
                    new_thread.start()
                    threads[i] = new_thread
                    logging.error('Thread restarted')
                
                # Stock and F&O markets closes at 15:30  
                elif(i > 6 and time_now.hour < 15):
                    logging.error('Time:%s',time_now)
                    logging.error('Thread died:%s',lstCmd[i][-26:])
                    new_thread = myThread(lstCmd[i])
                    new_thread.start()
                    threads[i] = new_thread
                    logging.error('Thread restarted')
                
                elif(i > 6 and time_now.hour == 15 and time_now.minute < 30):

                    logging.error('Time:%s',time_now)
                    logging.error('Thread died:%s',lstCmd[i][-26:])
                    new_thread = myThread(lstCmd[i])
                    new_thread.start()
                    threads[i] = new_thread
                    logging.error('Thread restarted')
        
        # After sometime the run has started write in the file today's run date 
        count = 0
        if(time_now.hour == 9 and time_now.minute == 20 and count == 0):
            
            date = datetime.date.today()
            
            exec_file_name = "/home/tariqanwarph/zerodha/Zerodha/last_execution_date.txt"
            with open(exec_file_name, 'w+') as infile:
                try:
                    # prev = json.load(infile)
                    infile.write(str(date))
                    count = count + 1
                except JSONDecodeError:
                    print("exception")
                    pass      

        # End the run after 17:00        
        if(time_now.hour == 17 and time_now.minute > 0):
            
            logging.info('Generating random crosschecks and mailing logs')
            gen_crosscheck_files()
            email_logs()
            logging.info('Run Ended at:%s', time_now)
            break 

if __name__ == "__main__":
    date = datetime.date.today()
    log_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",f"logs/run_{date}.log"))
    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    time_now = datetime.datetime.now()
    logging.info('Today date is %s',date)
    logging.info('Started run at %s',time_now)

    print(date)
    with open("last_execution_date.txt", 'a+') as infile:
        try:
            # prev = json.load(infile)
           infile.seek(0)
           a = infile.readline()
           print(a)
        except JSONDecodeError:
            print("exception")
            pass
    
    if(a == ''):
        dataflow_job()
        gen_tokens()
        celery_websocket()
        
    else:
        date_exec = datetime.datetime.strptime(a, "%Y-%m-%d")
        print(date_exec.date())
        
        if(date_exec.date() == date):
            
            celery_websocket()
            
        else:
            dataflow_job()
            gen_tokens()
            celery_websocket()
    
    # Update the file with run date
    with open("last_execution_date.txt", 'w+') as infile:
        try:
            # prev = json.load(infile)
           infile.write(str(date))
        except JSONDecodeError:
            print("exception")
            pass
    