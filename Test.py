from celery import Celery
from time import sleep
import winsound

app = Celery('tasks', broker = 'amqp://guest:guest@localhost:5672//', backend='db+sqlite:///db.sqlite3')

from configparser import ConfigParser
parser = ConfigParser()

parser.read('test.ini')

@app.task
def add_print(x):
    sleep(5)
    parser.set('new', 'key', 'value')	
    
    with open('test.ini', 'w') as configfile:
        
        parser.write(configfile)
    duration = 1000  # milliseconds
    freq = 440  # Hz
    winsound.Beep(freq, duration)
    
    print(x*2)

