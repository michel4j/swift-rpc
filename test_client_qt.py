
import time
import sys
import random
from threading import Thread

from PyQt5.QtWidgets import QApplication

from szrpc import log
from szrpc.client import Client

# use GObject based result class
Client.use('szrpc.result.qresult.QResult')
logger = log.get_module_logger('client')


class App(QApplication):

    def __init__(self):
        self.results = []
        self.client = Client('tcp://localhost:9990', heartbeat=1)
        super().__init__(sys.argv)

    @staticmethod
    def on_done(res, data):
        logger.info(f"DONE: {res}, {data!r}")

    @staticmethod
    def on_err(res, data):
        logger.info(f"ERROR: {res}, {data!r}")

    @staticmethod
    def on_update(res, data):
        logger.info(f"UPDATE: {res}, {data!r}")

    def monitor(self, res):
        res.connect('done', self.on_done)
        res.connect('update', self.on_update)
        res.connect('failed', self.on_err)
        self.results.append(res)

    def run(self):
        start_time = time.time()
        while not self.client.is_ready():
            logger.info('Waiting for connection ...')
            time.sleep(1)

        names = ['Joe', 'Jim', 'Janay', 'John']
        i = 0
        while True:
            if i % 2 == 0:
                self.monitor(self.client.hello_world(name=random.choice(names)))
            elif i % 3 == 0:
                self.monitor(self.client.progress(label=f'proc{i}'))
            else:
                self.monitor(self.client.date())
            i += 1
            time.sleep(0.5)

            # Remove completed results
            self.results = [res for res in self.results if not res.is_ready()]

            if i > 1_000_000:
                break

        while self.results:
            self.results = [res for res in self.results if not res.is_ready()]
            time.sleep(0.1)

        logger.info(f"Client done in {time.time() - start_time:.2f} seconds")
        self.quit()

    def start(self):
        Thread(target=self.run, daemon=True).start()
        self.exec()


if __name__ == '__main__':
    log.log_to_console()
    app = App()
    app.start()

