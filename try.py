from Test import add_print
from celery import Celery
from time import sleep

# for i in range():
result = add_print.delay(55)

print("here")
print("here1")
print("here2")
while (True):
    sleep(0.5)
    print(result.status)
    if(result.status == "SUCCESS"):
        break


    