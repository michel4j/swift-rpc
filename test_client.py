import time
import random

from szrpc import log
from szrpc.client import Client


logger = log.get_module_logger('client')
if __name__ == '__main__':
    log.log_to_console()
    client = Client('tcp://localhost:9990')

    def on_done(res, data):
        logger.info(f"{data!r}")

    def on_err(res, data):
        logger.info(f"FAILED: {str(res)} : {data!r}")

    def on_update(res, data):
        logger.info(f"{data}")

    while not client.is_ready():
        time.sleep(.001)

    results = []
    names = ['Joe', 'Jim', 'Janay', 'John']
    for i in range(2):
        c = random.choice([0, 1, 2])
        if c == 0:
            name = random.choice(names)
            res = client.hello_world(name=name)
        elif c == 1:
            res = client.date()
        elif c == 2:
            res = client.progress()

        res.connect('done', on_done)
        res.connect('update', on_update)
        res.connect('failed', on_err)
        results.append(res)
        time.sleep(.05)

    while results:
        results = [res for res in results if not res.is_ready()]
        time.sleep(0.1)

