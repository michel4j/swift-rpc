import time
import random

from szrpc import log
from szrpc.client import Client


logger = log.get_module_logger('client')
if __name__ == '__main__':
    log.log_to_console()
    client = Client('tcp://localhost:9990', methods=['hello_world', 'date', 'progress'], heartbeat=2)

    def on_done(res, data):
        logger.info(f"DONE: {res}, {data!r}")

    def on_err(res, data):
        logger.info(f"ERROR: {res}, {data!r}")

    def on_update(res, data):
        logger.info(f"UPDATE: {res}, {data!r}")

    def monitor(res, result_list):
        res.connect('done', on_done)
        res.connect('update', on_update)
        res.connect('failed', on_err)
        result_list.append(res)

    while not client.is_ready():
        time.sleep(.001)

    results = []
    names = ['Joe', 'Jim', 'Janay', 'John']
    for i in range(5):
        if i % 2 == 0:
            monitor(client.hello_world(name=random.choice(names)), results)
        if i % 3 == 0:
            monitor(client.progress(label=f'proc{i}'), results)

        monitor(client.date(), results)
        time.sleep(0.5)

    while results:
        results = [res for res in results if not res.is_ready()]
        time.sleep(0.1)

    # Wait for 20 seconds to test the heartbeat
    for i in range(20):
        time.sleep(1)

    logger.info("Client done")
