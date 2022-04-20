from datetime import datetime
import time
from szrpc import log
from szrpc.server import Server, Service, ResponseType


class MyService(Service):

    def remote__hello_world(self, request, name=None):
        request.reply(f'Please wait, {name}. This will take a while.')
        time.sleep(10)
        return f'Hello, {name}. How is your world today?'

    def remote__date(self, request):
        time.sleep(1)
        return f"Today's date is {datetime.now()}"

if __name__ == '__main__':
    service = MyService()
    log.log_to_console()
    server = Server(service=service, ports=(9990, 9991), instances=1)
    server.run(load_balancing=True)
