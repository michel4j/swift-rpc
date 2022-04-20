
from szrpc import log
from szrpc.server import Server, Service, WorkerManager

from test_server import MyService

if __name__ == '__main__':

    service = MyService()
    log.log_to_console()
    server = WorkerManager(service=service, backend="tcp://localhost:9991", workers=2)
    server.run()
