import asyncio
import msgpack
from async_timeout import timeout as Timeout
from .types import Types, Command, Code


class Connection:
    __slots__ = (
        'host', 'port', 'loop', 'open', '_reader', '_writer',
        '__reader_lock', '__connection_lock',
    )

    def __init__(self, host: str, port: int, loop: asyncio.AbstractEventLoop):
        self.host = host                    # type: str
        self.port = int(port)               # type: int
        self.loop = loop                    # type: asyncio.AbstractEventLoop
        self.open = False                   # type: bool
        self._reader = None                 # type: asyncio.StreamReader
        self._writer = None                 # type: asyncio.StreamWriter
        self.__reader_lock = asyncio.Lock(loop=self.loop)
        self.__connection_lock = asyncio.Lock(loop=self.loop)

    @asyncio.coroutine
    def connect(self):
        if self.open:
            return

        with (yield from self.__connection_lock):
            self._reader, self._writer = yield from asyncio.open_connection(
                self.host,
                self.port,
                loop=self.loop
            )

            self.open = True

    @asyncio.coroutine
    def read(self, n=-1):
        yield from self.connect()

        with (yield from self.__reader_lock):
            data = yield from self._reader.read(n)
            return data

    @asyncio.coroutine
    def readline(self):
        yield from self.connect()

        with (yield from self.__reader_lock):
            data = yield from self._reader.readline()
            return data

    @asyncio.coroutine
    def readexactly(self, n):
        yield from self.connect()

        with (yield from self.__reader_lock):
            data = yield from self._reader.readexactly(n)
            return data

    def write(self, data):
        return self._writer.write(data)

    def writelines(self, data):
        return self._writer.writelines(data)

    def can_write_eof(self):
        return self._writer.can_write_eof()

    def write_eof(self):
        return self._writer.write_eof()

    def get_extra_info(self, name, default=None):
        return self._writer.get_extra_info(name, default)

    def close(self):
        return self._writer.close()

    @asyncio.coroutine
    def drain(self):
        yield from self._writer.drain()


class Client:
    __slots__ = 'loop', 'remote_host', 'remote_port', 'connection', 'timeout'

    def __init__(self, host: str='127.0.0.1', port: int=2437,
                 loop: asyncio.AbstractEventLoop=None, timeout: int=5):

        self.loop = loop if loop else asyncio.get_event_loop()  # type: asyncio.AbstractEventLoop
        self.remote_host = host                                 # type: str
        self.remote_port = int(port)                            # type: int
        self.timeout = timeout                                  # type: int
        self.connection = Connection(host, port, loop)          # type: Connection

    @asyncio.coroutine
    def __get_response(self):
        code = Code.unpack((yield from self.connection.readexactly(len(Code))))

        length = Types.ulong.unpack((
            yield from self.connection.readexactly(len(Types.ulong))
        ))

        data = msgpack.loads((yield from self.connection.readexactly(length)))
        return code, data

    @staticmethod
    def __create_request(command: Command, request_data=None):
        payload = msgpack.dumps(request_data)
        length = len(payload)

        return command.pack() + Types.ulong.pack(length) + payload

    @asyncio.coroutine
    def request(self, command: Command, payload: dict, timeout=None):
        with Timeout(timeout or self.timeout, loop=self.loop):
            yield from self.connection.connect()
            self.connection.write(
                self.__create_request(command, payload)
            )
            yield from self.connection.drain()
            return (yield from self.__get_response())

    def map(self, timeout=None):
        return self.request(Command.MAP, timeout)

    def set(self, key, value, timeout=None):
        return self.request(Command.SET, {'value': value, 'key': key}, timeout=timeout)

    def get(self, key, timeout=None):
        return self.request(Command.GET, {'key': key}, timeout=timeout)


class NodeClient(Client):
    __slots__ = 'node_host', 'node_port'

    def __init__(self, *args, **kwargs):
        node = kwargs.pop('node')
        self.node_host = node.host
        self.node_port = node.port
        super(NodeClient, self).__init__(*args, **kwargs)

    def join(self, host, port, timeout=None):
        return self.request(Command.JOIN, dict(host=host, port=port), timeout=timeout)

    def replicate(self, timeout=None, **kwargs):
        return self.request(Command.REPLICATE, kwargs, timeout=timeout)
