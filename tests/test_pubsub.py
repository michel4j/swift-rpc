import socket
import time
from threading import Event

from szrpc.client import Client
from szrpc.server import Server, Service
from szrpc.transport import InMemoryNetwork


class EventfulService(Service):
    def remote__emit(self, request, topic='beamline.state', payload=None):
        self.publish(topic, payload)
        return 'published'


def _free_ports(count: int) -> list[int]:
    socks = []
    ports = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('127.0.0.1', 0))
        ports.append(s.getsockname()[1])
        socks.append(s)
    for s in socks:
        s.close()
    return ports


def test_inmemory_pubsub_multiple_clients_and_unsubscribe():
    net = InMemoryNetwork()

    with Server(
        EventfulService,
        instances=1,
        threaded=True,
        transport=net.broker(),
        worker_transport_factory=net.worker,
    ) as server:
        server.start()
        with (
            Client(transport=net.client(b'c-1')) as c1,
            Client(transport=net.client(b'c-2')) as c2,
        ):
            seen1 = []
            seen2 = []
            got1 = Event()
            got2 = Event()

            def on_ev1(topic, payload):
                seen1.append((topic, payload))
                got1.set()

            def on_ev2(topic, payload):
                seen2.append((topic, payload))
                got2.set()

            tok1 = c1.subscribe('beamline.', on_ev1)
            c2.subscribe('beamline.motor', on_ev2)

            res = c1.emit(topic='beamline.motor', payload={'pos': 12.5})
            assert res.wait(timeout=2)
            assert got1.wait(timeout=2)
            assert got2.wait(timeout=2)
            assert seen1 == [('beamline.motor', {'pos': 12.5})]
            assert seen2 == [('beamline.motor', {'pos': 12.5})]

            c1.unsubscribe(tok1)
            got1.clear()
            got2.clear()
            res2 = c1.emit(topic='beamline.motor', payload={'pos': 20.0})
            assert res2.wait(timeout=2)
            assert got2.wait(timeout=2)
            assert seen1 == [('beamline.motor', {'pos': 12.5})]
            assert seen2[-1] == ('beamline.motor', {'pos': 20.0})


def test_zmq_pubsub_end_to_end():
    front, back, pub, xsub = _free_ports(4)
    # Note: Server binds pub_port and pub_port + 1; pick an unused adjacent pair
    # by using front/back and finding a contiguous pair or just pub_port = pub.
    # For reliability in tests, retry if pub+1 is busy, or pick from a small range.
    for candidate in range(38000, 39000, 2):
        try:
            s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s1.bind(('127.0.0.1', candidate))
            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2.bind(('127.0.0.1', candidate + 1))
            s1.close()
            s2.close()
            pub_port = candidate
            break
        except OSError:
            continue

    with Server(
        EventfulService,
        ports=(front, back),
        instances=1,
        threaded=True,
        pub_port=pub_port,
    ) as server:
        server.start()
        with Client(
            f'tcp://127.0.0.1:{front}',
            linger=False,
            sub_address=f'tcp://127.0.0.1:{pub_port}',
        ) as client:
            received = []
            done = Event()

            def on_event(topic, payload):
                received.append((topic, payload))
                done.set()

            client.subscribe('engine.', on_event)
            # Allow ZMQ SUB subscription propagation
            time.sleep(0.15)

            res = client.emit(topic='engine.tick', payload={'step': 1})
            assert res.wait(timeout=2)
            assert done.wait(timeout=2)
            assert received == [('engine.tick', {'step': 1})]
