import multiprocessing
import random
import socket
import time
from threading import Event, Thread

import pytest

from szrpc.client import Client
from szrpc.namer import RandomGenerator, WorkerIdentityLease
from szrpc.server import Server, Service, Worker
from szrpc.transport import InMemoryNetwork


def _free_ports(count: int = 2) -> tuple[int, ...]:
    sockets = []
    ports = []
    try:
        for _ in range(count):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('127.0.0.1', 0))
            ports.append(s.getsockname()[1])
            sockets.append(s)
        return tuple(ports)
    finally:
        for s in sockets:
            s.close()


class IdentityTestService(Service):
    def remote__echo(self, request, msg='hi'):
        return msg


class CustomIdService(Service):
    def create_worker_id(self) -> bytes:
        return b'custom-override@HOST'

    def remote__echo(self, request, msg='hi'):
        return msg


def test_random_generator_isolated_and_deterministic():
    random.seed(4242)
    expected_global = [random.random() for _ in range(5)]

    random.seed(4242)
    g1 = RandomGenerator(seed=12345)
    names1 = [g1.generate_name() for _ in range(10)]
    actual_global = [random.random() for _ in range(5)]
    assert actual_global == expected_global

    random.seed(99999)
    g2 = RandomGenerator(seed=12345)
    names2 = []
    for _ in range(10):
        random.random()  # interleave global RNG calls
        names2.append(g2.generate_name())

    assert names1 == names2
    assert len(set(names1)) == 10


def test_worker_identity_lease_persists_and_reclaims_slots(tmp_path):
    l1 = WorkerIdentityLease.acquire('MyService', 'tcp://localhost:9991', host='NODE1', lock_dir=tmp_path)
    l2 = WorkerIdentityLease.acquire('MyService', 'tcp://127.0.0.1:9991', host='NODE1', lock_dir=tmp_path)
    l3 = WorkerIdentityLease.acquire('MyService', 'tcp://*:9991', host='NODE1', lock_dir=tmp_path)
    try:
        names = [l1.name, l2.name, l3.name]
        assert len(set(names)) == 3
        assert all(n.endswith('@NODE1') for n in names)

        # Release middle slot; next acquire reclaims it deterministically
        middle = l2.name
        l2.release()
        l2_reclaimed = WorkerIdentityLease.acquire(
            'MyService', 'tcp://localhost:9991', host='NODE1', lock_dir=tmp_path
        )
        try:
            assert l2_reclaimed.name == middle
        finally:
            l2_reclaimed.release()
    finally:
        l1.release()
        l2.release()
        l3.release()

    # Full restart reproduces the exact first slot name
    restarted = WorkerIdentityLease.acquire('MyService', 'tcp://localhost:9991', host='NODE1', lock_dir=tmp_path)
    try:
        assert restarted.name == names[0]
    finally:
        restarted.release()


def test_worker_identity_lease_specialized_methods_order_independent(tmp_path):
    la = WorkerIdentityLease.acquire(
        'MyService', 'tcp://localhost:9991', methods=('alpha',), host='NODE1', lock_dir=tmp_path
    )
    lb = WorkerIdentityLease.acquire(
        'MyService', 'tcp://localhost:9991', methods=('beta',), host='NODE1', lock_dir=tmp_path
    )
    name_a, name_b = la.name, lb.name
    assert name_a != name_b
    la.release()
    lb.release()

    # Launch in reverse order -> each specialized pool keeps its exact name
    lb2 = WorkerIdentityLease.acquire(
        'MyService', 'tcp://localhost:9991', methods=('beta',), host='NODE1', lock_dir=tmp_path
    )
    la2 = WorkerIdentityLease.acquire(
        'MyService', 'tcp://localhost:9991', methods=('alpha',), host='NODE1', lock_dir=tmp_path
    )
    try:
        assert lb2.name == name_b
        assert la2.name == name_a
    finally:
        la2.release()
        lb2.release()


def _hold_lease_subprocess(lock_dir_str: str, ready_queue, stop_conn) -> None:
    lease = WorkerIdentityLease.acquire(
        'SubprocService', 'tcp://localhost:9991', host='NODE1', lock_dir=lock_dir_str
    )
    ready_queue.put(lease.name)
    try:
        stop_conn.poll(15)
    finally:
        stop_conn.close()
        lease.release()


def test_worker_identity_lease_multiprocess_and_crash_recovery(tmp_path):
    ctx = multiprocessing.get_context('spawn')
    ready_q = ctx.Queue()
    pipes = [ctx.Pipe(duplex=False) for _ in range(3)]
    procs = [
        ctx.Process(target=_hold_lease_subprocess, args=(str(tmp_path), ready_q, r_conn))
        for r_conn, _ in pipes
    ]

    names = []
    try:
        for p in procs:
            p.start()
            names.append(ready_q.get(timeout=10))

        assert len(set(names)) == 3

        # Abruptly terminate the first worker process (simulating crash)
        crashed_name = names[0]
        procs[0].terminate()
        procs[0].join(timeout=5)

        # Kernel releases flock immediately -> replacement reclaims crashed worker's exact name
        reclaimed = WorkerIdentityLease.acquire(
            'SubprocService', 'tcp://localhost:9991', host='NODE1', lock_dir=tmp_path
        )
        try:
            assert reclaimed.name == crashed_name
        finally:
            reclaimed.release()
    finally:
        for r_conn, w_conn in pipes:
            try:
                w_conn.send(True)
            except OSError:
                pass
            w_conn.close()
            r_conn.close()
        for p in procs:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)


def test_zmq_worker_restart_preserves_identity_and_handover(monkeypatch, tmp_path):
    monkeypatch.setenv('XDG_RUNTIME_DIR', str(tmp_path))
    fe_port, be_port = _free_ports(2)
    backend = f'tcp://localhost:{be_port}'

    with Server(IdentityTestService, ports=(fe_port, be_port), instances=0) as server:
        server.start(proxy='balancing')

        # First worker run
        w1 = Worker(backend, IdentityTestService())
        t1 = Thread(target=w1.run, daemon=True)
        t1.start()

        with Client(f'tcp://localhost:{fe_port}') as client:
            res1 = client.echo(msg='first')
            assert res1.wait(timeout=3)
            assert res1.results == 'first'
            first_id = w1.worker_id
            assert first_id is not None

            # Stop first worker and immediately start replacement -> same identity + handover
            w1.stop()
            t1.join(timeout=3)

            w2 = Worker(backend, IdentityTestService())
            t2 = Thread(target=w2.run, daemon=True)
            t2.start()
            try:
                res2 = client.echo(msg='second')
                assert res2.wait(timeout=3)
                assert res2.results == 'second'
                assert w2.worker_id == first_id
            finally:
                w2.stop()
                t2.join(timeout=3)


def test_worker_explicit_id_and_service_override(monkeypatch, tmp_path):
    monkeypatch.setenv('XDG_RUNTIME_DIR', str(tmp_path))
    fe_port, be_port = _free_ports(2)
    backend = f'tcp://localhost:{be_port}'

    with Server(CustomIdService, ports=(fe_port, be_port), instances=0) as server:
        server.start(proxy='round-robin')

        w_override = Worker(backend, CustomIdService())
        w_explicit = Worker(backend, IdentityTestService(), worker_id='explicit-gpu@HOST')
        t1 = Thread(target=w_override.run, daemon=True)
        t2 = Thread(target=w_explicit.run, daemon=True)
        t1.start()
        t2.start()
        try:
            deadline = time.time() + 3
            while (w_override.worker_id is None or w_explicit.worker_id is None) and time.time() < deadline:
                time.sleep(0.02)
            assert w_override.worker_id == b'custom-override@HOST'
            assert w_explicit.worker_id == b'explicit-gpu@HOST'
        finally:
            w_override.stop()
            w_explicit.stop()
            t1.join(timeout=3)
            t2.join(timeout=3)


def test_broker_fast_restart_same_worker_id_recovers_balancing_and_monitor():
    mon_mod = pytest.importorskip('szrpc.monitor')
    entered = Event()
    release_old = Event()

    class BlockingService(Service):
        def remote__work(self, request, block: bool = False):
            if block:
                entered.set()
                release_old.wait(timeout=5)
            return 'done'

    net = InMemoryNetwork()
    monitor = mon_mod.Monitor(service_info={'description': 'FastRestart', 'methods': ('work',)})
    try:
        with Server(BlockingService, instances=0, transport=net.broker(), monitor=monitor) as server:
            st = Thread(target=server.run, kwargs={'proxy': 'balancing'}, daemon=True)
            st.start()

            w1 = Worker('', BlockingService(), transport=net.worker(b'steady-worker@HOST'))
            t1 = Thread(target=w1.run, daemon=True)
            t1.start()

            with Client(transport=net.client(b'c-1')) as client:
                # Occupy worker 1 so it is removed from balancing worker_queue
                stuck_res = client.work(block=True)
                assert entered.wait(timeout=2)

                # Kill worker 1 without completing the request, then immediately reconnect same worker_id
                w1.stop()
                t1.join(timeout=2)

                w2 = Worker('', BlockingService(), transport=net.worker(b'steady-worker@HOST'))
                t2 = Thread(target=w2.run, daemon=True)
                t2.start()
                try:
                    # Wait briefly for w2's initial heartbeat (new session) to be processed
                    res2 = client.work(block=False)
                    assert res2.wait(timeout=2)
                    assert res2.results == 'done'

                    data = monitor.get_data()
                    assert data['stats']['total_errors'] == 1
                    assert data['active_calls'] == []
                    assert 'steady-worker@HOST' in data['workers']
                finally:
                    release_old.set()
                    w2.stop()
                    t2.join(timeout=2)
                    _ = stuck_res

            st.join(timeout=2)
    finally:
        release_old.set()
        monitor.close()
