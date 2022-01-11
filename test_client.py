import time


from szrpc import log
from szrpc.client import Client

if __name__ == '__main__':

    client = Client('tcp://localhost:9990')

    log.log_to_console()

    def on_done(res, data):
        print(res, data)

    def run_one(client):
        res = client.hello_world(name='Michel')
        res.connect('done', on_done)
        return True
    time.sleep(5)
    while True:
        run_one(client)

