import logging
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
        logger.info(f"{data!r}")

    def on_err(res, data):
        logger.info(f"FAILED: {str(res)} : {data!r}")

    def on_update(res, data):
        logger.info(f"{data}")

    path = Path(sys.argv[1])
    count = 2

    # res = client.process_mx(
    #     directory=f'/tmp/proc-{count}',
    #     file_names=[str(path)],
    #     user_name='michel'
    # )
    #
    #
    #
    # res.connect('done', on_done)
    # res.connect('update', on_update)
    # res.connect('failed', on_err)

    # res1 = client.signal_strength(
    #     type='file',
    #     directory='/data/Xtal/643',
    #     template='A1_2_{:05d}.cbf',
    #     first=1,
    #     num_frames=1,
    #     user_name='michel'
    # )
    res1 = client.signal_strength(
        type='file',
        directory='/data/Xtal/IDP05511_4noh/data/',
        template='idp05511_1sm-b_{:03d}.img',
        first=1,
        num_frames=10,
        user_name='michel'
    )

    res1.connect('done', on_done)
    res1.connect('update', on_update)
    res1.connect('failed', on_err)

    Gtk.main()

