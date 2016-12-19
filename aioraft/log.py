import abc
import asyncio


class LogEntry:
    """ base class of all logs """
    __slots__ = 'node', 'committed', 'raft_index'

    repr_attrs = {'raft_index', 'committed', 'action'}

    def __init__(self, node, raft_index, commited=False):
        self.node = node
        self.committed = commited
        self.raft_index = raft_index

    @asyncio.coroutine
    def replicate(self):
        repr_ = dict()
        for attr in self.repr_attrs:
            repr_[attr] = getattr(self, attr)
        done, pending = yield from self.node.broadcast(
            'replicate', repr_, wait_majority=True
        )

        if done is not None and not self.committed:
            self.committed = True
            data = self.commit()
            yield from self.replicate()
            return data

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError


class SetLogEntry(LogEntry):
    repr_attrs = frozenset(LogEntry.repr_attrs | {'key', 'value'})
    action = 'set'

    def __init__(self, node, raft_index, key, value, commited=False):
        self.key = key
        self.value = value
        super(SetLogEntry, self).__init__(node, raft_index, commited)

    def commit(self):
        self.node.raft_index = self.raft_index
        entry = self.node.data.set_value(self.key, self.value)
        return entry


action_map = {
    'set': SetLogEntry,
}
