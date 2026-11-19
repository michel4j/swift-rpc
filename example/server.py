from datetime import datetime
import time
import random
from szrpc import log
from szrpc.server import Server, Service, ServiceFactory
from threading import Lock


logger = log.get_module_logger(__name__)


class MyService(Service):
    """
    Example Service for Testing
    """

    def __init__(self, arg1=1, arg2=2, arg3=3.3, arg4='four'):
        super().__init__()
        self.arg1 = arg1
        self.arg2 = arg2
        self.arg3 = arg3
        self.arg4 = arg4
        self.lock = Lock()

    def remote__hello_world(self, request, name=None):
        request.reply(f'Please wait, {name}. This will take a while.')
        time.sleep(random.randint(1, 5))
        return f'Hello, {name}. How is your world today?'

    def remote__date(self, request):
        time.sleep(random.randint(1, 3))
        return f"Today's date is {datetime.now()}"

    def remote__error(self, request):
        time.sleep(random.randint(1, 10))
        value = random.random()
        if value > 0.8:
            raise ValueError('This method raises an error randomly!')
        return f"Random number is {value}"

    def remote__progress(self, request, label='test'):
        total = random.randint(2, 20)
        with self.lock:
            for i in range(total):
                request.reply(f'{label} {i*100//total}% complete')
                time.sleep(random.random() * 0.1)
        return f"Progress done"


if __name__ == '__main__':
    log.log_to_console()
    factory = ServiceFactory(MyService, arg1=2, arg2=3, arg3=4.3, arg4='five')
    server = Server(factory, ports=(9990, 9991), instances=0, monitor_port=8080)
    server.run(proxy='round-robin')
