from datetime import datetime
import time
from szrpc import log
from szrpc.server import Server, Service, ServiceFactory

import numpy
import os

logger = log.get_module_logger(__name__)


class MyService(Service):

    def __init__(self, arg1=1, arg2=2, arg3=3.3, arg4='four'):
        super().__init__()
        self.arg1 = arg1
        self.arg2 = arg2
        self.arg3 = arg3
        self.arg4 = arg4
        numpy.random.seed((os.getpid() * int(time.time())) % 123456789)
        self.state = numpy.random.random_integers(0, 10, 20)

    def remote__hello_world(self, request, name=None):
        request.reply(f'Please wait, {name}. This will take a while.')
        time.sleep(10)
        return f'Hello, {name}. How is your world today?'

    def remote__date(self, request):
        time.sleep(1)
        return f"Today's date is {datetime.now()}"

    def remote__progress(self, request):
        for i in range(10):
            request.reply(f'{i*10}% complete')
            time.sleep(0.1)
        return f"Progress done"


if __name__ == '__main__':
    log.log_to_console()
    factory = ServiceFactory(MyService, arg1=2, arg2=3, arg3=4.3, arg4='five')
    server = Server(factory, ports=(9990, 9991), instances=0)
    server.run(balancing=True)
