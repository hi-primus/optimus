from celery import Celery

app = Celery('tasks', broker='pyamqp://guest@localhost//')

# To run
# Install erglang
# Install rabiitmq
# >> celery -A tasks worker --loglevel=info
@app.task
def add(x, y):
    return x + y
