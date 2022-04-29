=======================================
Swift RPC - Simple ZeroMQ RPC in Python
=======================================

Overview
========
Swift RPC (szrpc) is a framework for creating remote python servers and and clients able to connect to them.
It uses ZeroMQ for socket communications, and MessagePack for serialization. The key features which distinguish it from
other existing solutions are:

- Simple and clean API for creating clients, servers
- Servers can support one or more workers running on the same host or many distinct hosts, with transparent load balancing
- Supports multiple replies per request. Can be used to report progress for long running tasks or simply to send
  replies in chunks if the application needs it.
- Reply objects can be transparently integrated into Gtk or Qt graphical frameworks through signals.


Getting Started
===============
Installing inside a virtual environment as follows

::

    $ python -m venv myproject
    $ source myproject/bin/activate
    (myproject) $ pip3 install szrpc


Write your first RPC Service
============================
The following  example illustrates how simple it is to create one.

.. code-block:: python

    from szrpc.server import Service

    class MyService(Service):
        def remote__hello_world(self, request, name=None):
            """
            Single reply after a long duration
            """
            request.reply(f'Please wait, {name}. This will take a while.')
            time.sleep(10)
            return f'Hello, {name}. How is your world today?'

        def remote__date(self, request):
            """
            Single reply after a short duration
            """           time.sleep(0.1)
            return f"Today's date is {datetime.now()}"

        def remote__progress(self, request):
            for i in range(10):
                time.sleep(0.1)
                request.reply(f'{i*10}% complete')
            return f"Progress done"



The above example demonstrates the following key points applicable to Services:

- Sevices must be sub-classes of **szrpc.server.Service**.
- All methods prefixed with a `remote__` will be exposed remotely.
- the very first argument to all remote methods is a request instance which contains all the information about the request.
- The remaining arguments where present, must be keyword arguments. Positional arguments other than the initial `request`
  are not permitted.
- Remote methods may block.
- Multiple replies can be send back before the method completes. The return value will be the final reply sent to the client.

Running a Server instance
-------------------------
Once a service is defined, it can easily be used to start a server which can listen for incoming connections from multiple clients as follows:

.. code-block:: python

    from szrpc.server import Server

    if __name__ == '__main__':
       service = MyService()
       server = Server(service=service, ports=(9990, 9991))
       server.run()


This says that our server will be available at the TCP address 'tcp://localhost:9990' for clients, and at the address
'tcp://localhost:9991' for workers. For simple cases, you don't need to worry about workers but by default, one worker
created behind the scenes to provide the service, thus it is mandatory to specify both ports. Additionally,
you can change your mind and run additional workers at any point in the future on any host after the server is started.

To start the server with more than one worker on the local host, modify the `instances` keyword argument as follows:


.. code-block:: python

    server = Server(service=service, ports=(9990, 9991), instances=1)

It is possible to start the server with `instances = 0` however, it will obviously not be able to handle any requests
until at least one worker is started.

Starting External Workers
-------------------------
Starting external workers is very similar to starting Servers.

.. code-block:: python

    from szrpc import log
    from szrpc.server import Server, Service, WorkerManager

    from test_server import MyService

    if __name__ == '__main__':

        service = MyService()
        log.log_to_console()
        server = WorkerManager(service=service, backend="tcp://localhost:9991", instances=2)
        server.run()


In the above example, we are staring two instances of workers on this host which are connected to the backend address
of the main server.

Creating Clients
----------------

Clients are just as easy, if not easier to create.  Here is a test client for the above service.

.. code-block:: python

    import time
    from szrpc import log
    from szrpc.client import Client

    # Define response handlers
    def on_done(res, data):
        print(f"Done: {res} {data!r}")

    def on_err(res, data):
        print(f"Failed: {res} : {data!r}")

    def on_update(res, data):
        print(f"Update: {res} {data!r}")

    if __name__ == '__main__':
        log.log_to_console()
        client = Client('tcp://localhost:9990')

        # wait for client to be ready before sending commands
        while not client.is_ready():
            time.sleep(.001)

        res = client.hello_world(name='Joe')
        res.connect('done', on_done)
        res.connect('update', on_update)
        res.connect('failed', on_err)

Here we have defined a few handler functions to get called once the replies are received. A few things are noteworthy in
the above client code:

- The client automatically figures out from the server, which methods to generate. For this reason, you will get
  "InvalidAttribute" errors if the  initial handshake has not completed before method calls are made. For most production
  situations, this is not a problem but in the example above, we wait until the `client.is_ready()` returns `True` before
  proceeding.
- The method names at the client end do not nave the `remote__` prefix. This means, overriding remote methods in the client
  will clobber the name.
- Only key-worded arguments are allowed for remote methods.
- Results are delivered asynchronously.  To write synchronous code, you can call the `res.wait()` method on `Result` objects.


There are three signal types corresponding to the three types of replies a server can send:

'done'
    the server has completed processing the request, no further replies should be expected for this request

'update'
    partial data is has been received for the request. More replies should be expected.

'failed'
    The request has failed. No more replies should be expected.

Handler functions take two arguments, the first is always the `result` object, which is an instance of **szrpc.result.Result**,
and the second is the decoded message from the server.

Result Classes
--------------
All results are instances of **szrpc.result.Result** or sub-classes thereof. The types of result objects produced can be changed to allow better integration with various frameworks.
Presently, alternatives are available Gtk, Qt as well as a pure Python-based class. The pure Python result class is the default but it can easily be changed as follows.

.. code-block:: python


    from szrpc.result.gresult import GResult
    import szrpc.client

    szrpc.client.use(GResult)

    my_client = szrpc.client.Client('tcp://localhost:9990')

All subsequent result objects will be proper GObjects usable with the Gtk Main loop.
