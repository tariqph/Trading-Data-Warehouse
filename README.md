# Trading Data Warehouse

This project is designed and implemented to automatically collect realtime streaming trading data on a minute interval from a broker for over 5000 instruments and house it all in Google BigQuery for analysis and informing trade.
The project is mainly written in python. It utilizes RabbitMQ as a message broker and Celery as a distributed task queue worker at the local(VM) side. 
On Cloud's side, the data is published on Cloud PubSub which is processed by Cloud Dataflow and streamed into BigQuery in realtime. 


Below is a flowchart showing the data pipeline and services utilized.

![data warehouse drawio (2)](https://user-images.githubusercontent.com/14332590/157233498-a50276ce-b9e4-4f21-8062-a63f555422b2.png)
