import abc
import asyncio
import msgpack
import random
import logging
from math import ceil
from .types import Types, Command, Code
from .client import NodeClient
from .log import action_map, SetLogEntry
from .entry import DirEntry


logger = logging.getLogger(__name__)


class Node:
    __slots__ = ('host', 'port', 'loop', 'peers', 'raft_index', '_leader_node',
                 '_leader', 'heartbeat_timeout', 'term', 'current_term', 'data',
                 '_clients', 'pending_logs', '_closed')

    implements = frozenset({'map'})

    def __init__(self, host, port, peers=None, loop=None):
        self.host = host
        self.port = port
        self.loop = loop if loop else asyncio.get_event_loop()
        self.peers = set(peers) if peers is not None else set()

        self.raft_index = 0

        self._leader_node = tuple()
        self._leader = None

        self.heartbeat_timeout = 0.5

        self.term = 0
        self.current_term = 0

        self.data = DirEntry('root', node=self)

        self.pending_logs = {}

        self._closed = self.loop.create_future()
        self._clients = [NodeClient(*peer, loop=self.loop, timeout=0.5, node=self) for peer in self.peers]

    @asyncio.coroutine
    def response(self, writer: asyncio.StreamWriter, code: Code, response):
        payload = msgpack.dumps(response)
        length = len(payload)

        writer.write(code.pack() + Types.ulong.pack(length) + payload)
        yield from writer.drain()

    @property
    def term_timeout(self):
        # between 150 and 300
        return random.random() / 6.6 + 0.150

    @asyncio.coroutine
    def map(self, _):
        return Code.OK, dict(
            term=self.term,
            raft_index=self.raft_index,
            leader=self._leader_node,
            peers=list(self.peers)
        )

    @asyncio.coroutine
    @abc.abstractmethod
    def leader(self, **kwargs):
        raise NotImplementedError(
            '`get` not implemented on %s instances' % self.__class__
        )

    @asyncio.coroutine
    @abc.abstractmethod
    def get(self, **kwargs):
        raise NotImplementedError(
            '`get` not implemented on %s instances' % self.__class__
        )

    @asyncio.coroutine
    @abc.abstractmethod
    def set(self, **kwargs):
        raise NotImplementedError(
            '`set` not implemented on %s instances' % self.__class__
        )

    @asyncio.coroutine
    @abc.abstractmethod
    def term(self, **kwargs):
        raise NotImplementedError(
            '`term` not implemented on %s instances' % self.__class__
        )

    @asyncio.coroutine
    @abc.abstractmethod
    def beat(self, **kwargs):
        raise NotImplementedError(
            '`beat` not implemented on %s instances' % self.__class__
        )

    @asyncio.coroutine
    @abc.abstractmethod
    def join(self, **kwargs):
        raise NotImplementedError(
            '`join` not implemented on %s instances' % self.__class__
        )

    @asyncio.coroutine
    @abc.abstractmethod
    def leave(self, **kwargs):
        raise NotImplementedError(
            '`leave` not implemented on %s instances' % self.__class__
        )

    @asyncio.coroutine
    def _handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        logger.debug('handle')

        while True:
            cmd = Command.unpack((yield from reader.readexactly(len(Command))))
            method = cmd.key.lower()

            if method not in self.implements:
                yield from self.response(
                    writer,
                    Code.INTERNAL_SERVER_ERROR,
                    bytes('ERROR : method `%s` not implemented\n' % method.strip())
                )

                continue

            args_length = Types.ulong.unpack((
                yield from reader.readexactly(len(Types.ulong))
            ))

            kwargs = msgpack.loads((yield from reader.readexactly(args_length)))

            try:
                code, result = yield from asyncio.coroutine(getattr(self, method))(**kwargs)
                yield from self.response(writer, code, result)
            except Exception as e:
                yield from self.response(writer, Code.INTERNAL_SERVER_ERROR, bytes('ERROR : %s' % e))
                logger.exception("Exception when handling request %r")


class Follower(Node):
    implements = frozenset(['get', 'set', 'map', 'replicate'])

    def __init__(self, *args, **kwargs):
        super(Follower, self).__init__(*args, **kwargs)
        self.loop.create_task(self.async_init())

    @asyncio.coroutine
    def async_init(self):
        yield from self.update_map()
        if self._leader:
            logger.info('joining')
            yield from self._leader.join(self.host, self.port)

        elif not self.peers:
            logger.info('No other peers, I am the leader')
            # no peers, promote oneself
            self.__class__ = Leader
            self._leader = None
            self.term = 1
            self._leader_node = (self.host, self.port)

    @asyncio.coroutine
    def broadcast(self, function, attrs, exclude=None, wait_majority=False):
        """ Broacast :arg:function to all clients except hosts in :arg:exclude.
            If :arg:wait_majority is True, return as soon as the majority of
            clients returned without error
            If :arg:wait_majority is an integer, return as soon as
            :arg:wait_majority clients returned without error
        """
        exclude = exclude if exclude is not None else []
        coros = [getattr(c, function)(**attrs)
            for c in self.clients
            if (c.host, c.port) not in exclude
        ]

        if wait_majority:
            # let the coro finnish even after we return
            coros = [asyncio.shield(c) for c in coros]
            if wait_majority is True:
                maj = ceil((len(coros) + len(exclude)) / 2.)
            else:
                maj = wait_majority
            success = 0
            while True:
                w = asyncio.wait(coros,
                    loop=self.loop, return_when=asyncio.FIRST_COMPLETED)
                try:
                    done, coros = yield from asyncio.wait_for(w,
                        loop=self.loop, timeout=self.heartbeat_timeout)
                except asyncio.TimeoutError:
                    [c.cancel() for c in coros]
                    return None, None
                success += len([d for d in done if d.exception() is None])
                if success >= maj:
                    return done, coros
                if success + len(coros) < maj:
                    # not any chance to succed
                    [c.cancel() for c in coros]
                    return False
        else:
            done, pending = yield from asyncio.wait(coros, loop=self.loop,
                timeout=self.heartbeat_timeout)
            [c.cancel() for c in pending]
            return done, pending

    def update_clients(self):
        current_clients = set([(c.remote_host, c.remote_port) for c in self._clients])
        new_clients = self.peers.difference(current_clients)
        for c in new_clients:
            self._clients.append(NodeClient(*c, loop=self.loop, node=self))

    @asyncio.coroutine
    def update_map(self):
        clients = self.clients
        if clients:
            term, leader = self.term, self._leader_node
            for c in clients:
                map_ = yield from c.map()
                result = msgpack.loads(map_.decode())
                print(result)
                for p in result['peers']:
                    self.peers.add(p)
                # chose the leader if it's in a higher term
                term, leader = max((term, leader), (result['term'], tuple(result['leader'])))
            self.term, self._leader_node = max((self.term, self._leader_node), (term, leader))
            self.update_clients()
            if self._leader_node:
                self._leader = NodeClient(*self._leader_node, loop=self.loop, node=self)

    def get(self, key):
        logger.debug('get %s' % key)
        data = self.data.get_entry(key, create=False)

        return Code.OK, dict(
            key=key,
            value=data.value,
            index=data.index
        )

    def replicate(self, **kwargs):
        index = kwargs['raft_index']
        log = self.pending_logs.get(index, None)

        if log is None:
            action = kwargs.pop('action')
            log = action_map[action](self, **kwargs)
            self.pending_logs[logger.raft_index] = log
        else:
            logger.commit()

        return Code.OK, None

    @asyncio.coroutine
    def join(self, writer, host, port):
        result = yield from self._leader.join(host, port)
        return Code.OK, result

    @property
    def clients(self):
        if self._leader:
            return [self._leader] + self._clients
        else:
            return self._clients


class Leader(Follower):
    implements = frozenset(Follower.implements | {'join'})

    @property
    def clients(self):
        return self._clients

    @asyncio.coroutine
    def join(self, host, port):
        self.peers.add((host, port))
        yield from self.set('_raft/peers/%s:%s' % (host, port), '')
        self.update_clients()

        return Code.OK, None

    @asyncio.coroutine
    def set(self, key, value):
        logger.debug('set %s %s' % (key, value))
        entry = SetLogEntry(self, self.next_index, key, value)

        if self._clients:
            yield from entry.replicate()
        else:
            entry.commit()

        return Code.OK, {'key': key, 'value': value, 'index': entry.raft_index}

    @property
    def next_index(self):
        self.raft_index += 1
        return self.raft_index


def init(host='127.0.0.1', port='2437', peers=None, loop=None):
    loop = loop if loop else asyncio.get_event_loop()
    node = Follower(host=host, port=port, peers=peers, loop=loop)
    server_coro = asyncio.start_server(node._handler, host, port, loop=loop)
    server = loop.run_until_complete(server_coro)
    return node, server


def close(server, loop: asyncio.AbstractEventLoop=None):
    loop = loop if loop else asyncio.get_event_loop()
    server.close()
    loop.run_until_complete(server.wait_closed())

