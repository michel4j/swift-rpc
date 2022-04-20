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
    names = ['Adele', 'Imani', 'Kayla', 'Michel']
    for i in range(30):
        if i % 2 == 0:
            name = random.choice(names)
            res = client.hello_world(name=name)
        else:
            res = client.date()
        res.connect('done', on_done)
        res.connect('update', on_update)
        res.connect('failed', on_err)
        results.append(res)
        time.sleep(1)

    while results:
        results = [res for res in results if not res.is_ready()]
        time.sleep(0.1)

