
from szrpc import log
from szrpc.server import ServiceFactory, WorkerManager

from test_server import MyService

if __name__ == '__main__':

    factory = ServiceFactory(MyService, arg1=2, arg2=3, arg3=4.3, arg4='five')

    log.log_to_console()
    manager = WorkerManager(factory, address="tcp://localhost:9991", instances=5)
    manager.run()
