import datetime
import time
from szrpc import log
from szrpc.server import Server, Service

if __name__ == '__main__':


    class MyService(Service):

        def remote__hello_world(self, request, name=None):
            time.sleep(5)
            return f'Hello, {name}. How is your world today?'

        def remote__date(self, request):
            time.sleep(1)
            return f"Today's date is {datetime.now()}"


    service = MyService()
    log.log_to_console()
    server = Server(service=service, port=9990)
    server.run()
