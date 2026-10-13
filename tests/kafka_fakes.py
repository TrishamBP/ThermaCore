"""In-memory stand-in for confluent_kafka.Producer, so unit tests need no broker.

It mimics the parts the publisher relies on: produce() queues locally (and can raise
BufferError when "full"), delivery callbacks fire only inside poll()/flush(), flush()
returns the number of messages still unconfirmed, and list_topics() returns metadata.
"""

from types import SimpleNamespace

from confluent_kafka import KafkaError, KafkaException


class FakeMessage:
    def __init__(self, topic, key, value, headers):
        self._topic, self._key, self._value, self._headers = topic, key, value, headers

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value

    def headers(self):
        return self._headers


class FakeProducer:
    def __init__(self, config, *, topic="datacenter.telemetry", partitions=3, fail_keys=(),
                 buffer_full_times=0, stuck_keys=(), metadata_error=None, unreachable=False, fail_all=False):
        self.config = config
        self.topic_name, self.partitions = topic, partitions
        self.fail_keys = {k.encode() if isinstance(k, str) else k for k in fail_keys}
        self.buffer_full_times = buffer_full_times
        # Messages for stuck keys never get a delivery callback (e.g. partition leader unreachable).
        self.stuck_keys = {k.encode() if isinstance(k, str) else k for k in stuck_keys}
        self.metadata_error, self.unreachable = metadata_error, unreachable
        self.fail_all = fail_all   # every delivery fails (e.g. DLQ broker down)
        self.pending: list[tuple[FakeMessage, object]] = []
        self.sent: list[FakeMessage] = []          # broker-confirmed
        self.flush_calls = 0

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        if self.buffer_full_times > 0:
            self.buffer_full_times -= 1
            raise BufferError("Local: Queue full")
        self.pending.append((FakeMessage(topic, key, value, headers), on_delivery))

    def _deliver(self) -> int:
        n = 0
        still_pending = []
        for msg, cb in self.pending:
            if msg.key() in self.stuck_keys:
                still_pending.append((msg, cb))
                continue
            if self.fail_all or msg.key() in self.fail_keys:
                cb(KafkaError(KafkaError._MSG_TIMED_OUT), msg)
            else:
                self.sent.append(msg)
                cb(None, msg)
            n += 1
        self.pending = still_pending
        return n

    def poll(self, timeout=0):
        return self._deliver()

    def flush(self, timeout=None):
        self.flush_calls += 1
        self._deliver()
        return len(self.pending)

    def __len__(self):
        return len(self.pending)

    def list_topics(self, topic=None, timeout=-1):
        if self.unreachable:
            raise KafkaException(KafkaError(KafkaError._TRANSPORT, "Connection refused"))
        if topic != self.topic_name:
            info = SimpleNamespace(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART), partitions={})
        else:
            info = SimpleNamespace(error=self.metadata_error,
                                   partitions={i: SimpleNamespace(id=i) for i in range(self.partitions)})
        return SimpleNamespace(topics={topic: info}, brokers={1: object()})


def factory(**kwargs):
    """Return (producer_factory, holder) where holder['producer'] is the created fake."""
    holder = {}

    def make(config):
        holder["producer"] = FakeProducer(config, **kwargs)
        return holder["producer"]
    return make, holder


# ---------------------------------------------------------------- consumer side

class FakeKafkaMessage:
    """Stand-in for confluent_kafka.Message as returned by Consumer.consume()."""

    def __init__(self, topic, partition, offset, value, key=None, error=None, timestamp_ms=1_700_000_000_000):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value, self._key, self._error, self._ts = value, key, error, timestamp_ms

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return self._error

    def timestamp(self):
        return (1, self._ts)          # (TIMESTAMP_CREATE_TIME, ms)


class FakeConsumer:
    """Scripted consumer: consume() returns the queued batches one by one, then [].

    commit_errors: KafkaError objects raised (one per commit call) before commits succeed.
    on_consume: optional callback(consumer, call_number) to simulate signals mid-run.
    """

    def __init__(self, config, *, batches=(), topics=("datacenter.telemetry", "datacenter.telemetry.dlq"),
                 commit_errors=(), on_consume=None, unreachable=False, assign_on_call=1, partitions=(0, 1, 2)):
        self.config = config
        self.batches = [list(b) for b in batches]
        self.topics = set(topics)
        self.commit_errors = list(commit_errors)
        self.on_consume, self.unreachable = on_consume, unreachable
        self.commits: list[list[tuple]] = []        # successful commits: [(topic, partition, offset), ...]
        self.commit_attempts = 0
        self.consume_calls = 0
        self.closed = False
        self.subscribed = None
        self.callbacks = {}
        self.assign_on_call, self.partitions = assign_on_call, partitions   # None = never assigned

    def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None):
        self.subscribed = list(topics)
        self.callbacks = {"assign": on_assign, "revoke": on_revoke, "lost": on_lost}

    def consume(self, num_messages=1, timeout=-1):
        self.consume_calls += 1
        if self.assign_on_call == self.consume_calls and self.callbacks.get("assign"):
            from confluent_kafka import TopicPartition
            self.callbacks["assign"](self, [TopicPartition(self.subscribed[0], p) for p in self.partitions])
        if self.on_consume is not None:
            self.on_consume(self, self.consume_calls)
        if not self.batches:
            return []
        batch = self.batches.pop(0)
        assert len(batch) <= num_messages, "test batch larger than the consumer asked for"
        return batch

    def commit(self, offsets=None, asynchronous=True):
        self.commit_attempts += 1
        assert asynchronous is False, "the consumer must commit synchronously"
        if self.commit_errors:
            raise KafkaException(self.commit_errors.pop(0))
        self.commits.append([(tp.topic, tp.partition, tp.offset) for tp in offsets])

    def committed_offsets(self) -> dict:
        """Latest committed offset per (topic, partition)."""
        out = {}
        for commit in self.commits:
            for topic, partition, offset in commit:
                out[(topic, partition)] = offset
        return out

    def list_topics(self, topic=None, timeout=-1):
        if self.unreachable:
            raise KafkaException(KafkaError(KafkaError._TRANSPORT, "Connection refused"))
        return SimpleNamespace(topics={t: SimpleNamespace(error=None, partitions={0: None}) for t in self.topics},
                               brokers={1: object()})

    def close(self):
        self.closed = True


def consumer_factory(**kwargs):
    holder = {}

    def make(config):
        holder["consumer"] = FakeConsumer(config, **kwargs)
        return holder["consumer"]
    return make, holder
