import time
import sys
from pathlib import Path

import gi
gi.require_version('Gtk', '3.0')

from szrpc.result.gresult import GResult
import szrpc.client
szrpc.client.use(GResult)
from szrpc import log
from szrpc.client import Client


from gi.repository import Gtk

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

    Gtk.main()

    while not res.is_ready():
        logger.log(log.IMPORTANT, 'Waiting for results ...')
        Gtk.main_iteration()
        time.sleep(0.01)

