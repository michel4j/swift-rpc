import time
import sys
from pathlib import Path
from szrpc import log
from szrpc.client import Client


logger = log.get_module_logger('client')
if __name__ == '__main__':
    log.log_to_console()
    client = Client('tcp://localhost:9990')

    def on_done(res, data):
        logger.info(f"DONE: {res} : {data!r}")

    def on_err(res, data):
        logger.info(f"FAILED: {str(res)} : {data!r}")

    def on_update(res, data):
        logger.debug(f"{res} : {data!r}")


    path = Path(sys.argv[1])
    count = 1
    res = client.process_mx(
        directory=f'/tmp/proc-{count}',
        file_names=[str(path)],
        user_name='michel'
    )

    res.connect('done', on_done)
    res.connect('update', on_update)
    res.connect('failed', on_err)

    while not res.is_ready():
        logger.log(log.IMPORTANT, 'Waiting for results ...')
        time.sleep(0.1)

