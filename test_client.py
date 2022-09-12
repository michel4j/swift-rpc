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

    def monitor(res, results):
        res.connect('done', on_done)
        res.connect('update', on_update)
        res.connect('failed', on_err)
        results.append(res)

    while not client.is_ready():
        time.sleep(.001)

    results = []
    names = ['Joe', 'Jim', 'Janay', 'John']
    for i in range(15):
        if i % 2 == 0:
            monitor(client.hello_world(name=random.choice(names)), results)
        if i % 3 == 0:
            monitor(client.progress(label=f'proc{i}'), results)

        monitor(client.date(), results)
        time.sleep(.05)

    while results:
        results = [res for res in results if not res.is_ready()]
        time.sleep(0.1)

