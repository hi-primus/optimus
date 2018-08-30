from celery import Celery
import requests
import os

# Create the app and set the broker location (RabbitMQ)
app = Celery('worker',
             backend='rpc://',
             broker='redis://localhost:6379')


@app.task
def download(url):
    response = requests.get(url)
    data = response.text()
    print(data)