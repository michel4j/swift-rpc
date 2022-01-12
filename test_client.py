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
        logger.info(f"{data!r}")

    def on_err(res, data):
        logger.info(f"FAILED: {str(res)} : {data!r}")

    def on_update(res, data):
        logger.info(f"{data}")

    res = client.signal_strength(
        type='file',
        directory='/data/Xtal/CLS0026',
        template='CLS0026-5_{:04d}.cbf',
        first=1,
        num_frames=10,
        user_name='michel'
    )

    # res = client.signal_strength(
    #     type='file',
    #     directory='/data/Xtal/643',
    #     template='A1_2_{:05d}.cbf',
    #     first=1,
    #     num_frames=15,
    #     user_name='michel'
    # )

    res.connect('done', on_done)
    res.connect('update', on_update)
    res.connect('failed', on_err)

    while not res.is_ready():
        time.sleep(0.1)

