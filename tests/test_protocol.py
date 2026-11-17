import queue

from szrpc.protocol import Request, Response, ResponseType
from szrpc import server as szrpc_server


def test_request_roundtrip():
    req = Request(b'client-1', b'client-1-req1', 'add', {'x': 2, 'y': 3})
    payload = req.encode_payload()
    assert len(payload) == 3

    decoded = Request.create(b'client-1', *payload)
    assert decoded.client_id == b'client-1'
    assert decoded.request_id == b'client-1-req1'
    assert decoded.method == 'add'
    assert decoded.kwargs == {'x': 2, 'y': 3}


def test_request_reply_enqueues_response():
    q = queue.Queue()
    req = Request(b'c1', b'c1-r1', 'echo', {}, reply_to=q)
    resp = req.reply({'ok': True}, ResponseType.DONE)

    assert q.get_nowait() is resp
    assert resp.type == ResponseType.DONE
    assert resp.content == {'ok': True}


def test_response_roundtrip_and_heartbeat():
    resp = Response(b'c1', b'c1-r1', ResponseType.UPDATE, [1, 2])
    decoded = Response.decode(b'c1', resp.encode_payload())
    assert decoded.client_id == b'c1'
    assert decoded.request_id == b'c1-r1'
    assert decoded.type == ResponseType.UPDATE
    assert decoded.content == [1, 2]

    hb = Response.heartbeat_message(methods=('add',))
    assert hb.type == ResponseType.HEARTBEAT
    assert list(hb.content) == ['add']
    assert Response.heartbeat(methods=('add',)) == hb.parts()


def test_server_reexports_protocol_symbols():
    assert szrpc_server.Request is Request
    assert szrpc_server.Response is Response
    assert szrpc_server.ResponseType is ResponseType
