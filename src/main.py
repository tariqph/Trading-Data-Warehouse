import os
import threading

# Generating Access Tokens
# os.system('''cd "G:\DS - Competitions and projects\Zerodha\src\\access_config" & python generate_access_token.py''')
# # Generating instrument tokens
# os.system('''cd "G:\DS - Competitions and projects\Zerodha\src\\access_config" & python generate_all_tokens.py''')

class myThread (threading.Thread):
    def __init__(self, command):
        threading.Thread.__init__(self)
        self.cmd = command

    def run(self):
        print ("Starting " + self.cmd)
        os.system(self.cmd)
        print ("Exiting " + self.cmd)

lstCmd=['cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_f  worker -Q fando --concurrency=1  --loglevel=info -P  eventlet -n worker1@%h', 
        'cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_1  worker -Q stock1 --concurrency=1  --loglevel=info -P  eventlet -n worker2@%h',
        'cd "G:\DS - Competitions and projects\Zerodha\src\publish_database" & celery -A publish_pubsub_2  worker -Q stock2 --concurrency=1  --loglevel=info -P  eventlet -n worker3@%h',
        'cd "G:\DS - Competitions and projects\Zerodha\src\\ticker" & python save_data_FandO.py',
        'cd "G:\DS - Competitions and projects\Zerodha\src\\ticker" & python save_data_stock1.py',
        'cd "G:\DS - Competitions and projects\Zerodha\src\\ticker" & python save_data_stock2.py'
        ]   

# Create new threads
thread1 = myThread(lstCmd[0])
thread2 = myThread(lstCmd[1])
thread3 = myThread(lstCmd[2])
thread4 = myThread(lstCmd[3])
thread5 = myThread(lstCmd[4])
thread6 = myThread(lstCmd[5])


# Start new Threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
