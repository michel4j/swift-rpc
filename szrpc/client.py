import re
import time
import uuid
from queue import Queue
from threading import Thread

import zmq

import log
from server import ResponseType, Request, Response

logger = log.get_module_logger(__name__)


class Client(object):
    def __init__(self, address):
        self.client_id = str(uuid.uuid4())
        info = re.match(r'^(?P<url>...://[^:]+):(?P<port>\d+)$', address).groupdict()
        self.url = info['url']
        self.port = int(info['port'])
        self.req_url = f'{self.url}:{self.port}'
        self.rep_url = f'{self.url}:{self.port + 1}'
        self.requests = Queue(maxsize=1000)
        self.start()

    def start(self):
        sender = Thread(target=self.send_requests, daemon=True)
        sender.start()
        receiver = Thread(target=self.monitor_responses, daemon=True)
        receiver.start()

    def call_remote(self, method: str, kwargs: dict = None):
        request_id = str(uuid.uuid4())
        kwargs = {} if kwargs is None else kwargs
        self.requests.put(
            Request(self.client_id, request_id, method, kwargs)
        )
        return request_id

    def get_api(self):
        return self.call_remote('get_api')

    def send_requests(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect(self.req_url)
        logger.debug(f'Sending requests to {self.req_url}...')

        while True:
            request = self.requests.get()
            socket.send_multipart(
                request.parts()
            )
            time.sleep(0.001)

    def monitor_responses(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, self.client_id)
        socket.setsockopt_string(zmq.SUBSCRIBE, 'heartbeat')
        socket.connect(self.rep_url)
        logger.debug(f'Receiving replies from {self.rep_url}...')

        while True:
            reply_data = socket.recv_multipart()
            response = Response.create(*reply_data)
            if response.type == ResponseType.HEARTBEAT:
                logger.warning('HEARTBEAT')
            elif response.type == ResponseType.ERROR:
                logger.error(f'Request failed: {response.client_id}|{response.request_id}')
            else:
                logger.debug(f'Request received: {response.client_id}|{response.request_id}')
            print(response.content)
            time.sleep(0.001)


if __name__ == '__main__':
    client = Client('tcp://localhost:9990')
    log.log_to_console()
    while True:
        req_id = client.get_api()
        time.sleep(1)
