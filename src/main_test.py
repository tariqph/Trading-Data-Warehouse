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


def email_logs():
    fromaddr = "johnadamsrealno1@gmail.com"
    toaddr = "tariqanwarph@gmail.com"
    
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
    log_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",f"logs/run_test_{date}.log"))

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
    
    # creates SMTP session
    s = smtplib.SMTP('smtp.gmail.com', 587)
    
    # start TLS for security
    s.starttls()
    
    # Authentication
    s.login(fromaddr, "786920taR")
    
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
    table_name = 'new_day_test' + str(now.day)
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
    for i in tqdm(range(10)):
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
    
    # Commands to run parallely
    lstCmd=['cd "' + worker +'" && celery -A publish_pubsub_1  worker -Q conn_7 --concurrency=1  --loglevel=info -P  eventlet -n worker1@%h', 
            'cd "' + worker +'" && celery -A publish_pubsub_1  worker -Q conn_2 --concurrency=1  --loglevel=info -P  eventlet -n worker2@%h',
            'cd "' + worker +'" && celery -A publish_pubsub_1  worker -Q conn_3 --concurrency=1  --loglevel=info -P  eventlet -n worker3@%h',
            'cd "' + worker +'" && celery -A publish_pubsub_1  worker -Q conn_4 --concurrency=1  --loglevel=info -P  eventlet -n worker4@%h', 
            'cd "' + worker +'" && celery -A publish_pubsub_1  worker -Q conn_5 --concurrency=1  --loglevel=info -P  eventlet -n worker5@%h',
            'cd "' + worker +'" && celery -A publish_pubsub_1  worker -Q conn_8 --concurrency=1  --loglevel=info -P  eventlet -n worker6@%h',
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

     
    while True:
        time.sleep(30)
        time_now = datetime.datetime.now()
        for i in range(len(threads)):
            if threads[i].is_alive():
                continue
                
            else:
                # The currency data is collected using conn_1 so it runs till 17:00
                if(i <= 6 and time_now.hour <= 17 and time_now.minute <=0
                and time_now.second <= 0 ):

                    logging.info('Time:%s',time_now)
                    logging.info('Thread died:%s',lstCmd[i])
                    new_thread = myThread(lstCmd[i])
                    new_thread.start()
                    threads[i] = new_thread
                    logging.info('Thread restarted')
                
                # Stock and F&O markets closes at 15:30  
                elif(i > 6 and time_now.hour <= 15 and time_now.minute <=30
                and time_now.second <=0):

                    logging.info('Time:%s',time_now)
                    logging.info('Thread died:%s',lstCmd[i])
                    new_thread = myThread(lstCmd[i])
                    new_thread.start()
                    threads[i] = new_thread
                    logging.info('Thread restarted')

        # End the run after 17:00        
        if(time_now.hour == 17 and time_now.minute > 56):
            logging.info('Run Ended at:%s', time_now)
            # email_logs()
            break 

if __name__ == "__main__":
    date = datetime.date.today()
    log_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",f"logs/run_test_{date}.log"))
    logging.basicConfig(filename=log_file, level=logging.INFO)
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
        # dataflow_job()
        gen_tokens()
        celery_websocket()
        
    else:
        date_exec = datetime.datetime.strptime(a, "%Y-%m-%d")
        print(date_exec.date())
        
        if(date_exec.date() == date):
            dataflow_job()
            # celery_websocket()


        else:
            # dataflow_job()
            gen_tokens()
            celery_websocket()
    
    
    with open("last_execution_date.txt", 'w+') as infile:
        try:
            # prev = json.load(infile)
           infile.write(str(date))
        except JSONDecodeError:
            print("exception")
            pass