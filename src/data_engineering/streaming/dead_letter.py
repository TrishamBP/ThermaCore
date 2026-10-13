"""Dead-letter records for messages that can never be stored (topic `datacenter.telemetry.dlq`).

A DLQ record is a JSON envelope around the untouched original bytes:

    {"dlq_schema_version": "1.0", "failure_reason": "schema_violation",
     "failure_details": ["metrics/pue: None is not of type 'number'"],
     "failed_at": "2026-10-13T08:00:00.123456Z", "consumer_group": "dco-telemetry-sink",
     "source_topic": "datacenter.telemetry", "source_partition": 1, "source_offset": 42,
     "source_timestamp_ms": 1791874301330, "source_key": "SIM-NA-003",
     "original_payload_utf8": "{...}", "original_payload_base64": null}

`original_payload_base64` is used only when the original bytes are not valid UTF-8.
The record is keyed by the original key, so DLQ ordering per asset matches the source.

A record counts as published only after the broker confirms it (delivery callback).
"""

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from data_engineering.streaming.event_validation import Rejection

DLQ_SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class DeadLetter:
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]]
    source_partition: int
    source_offset: int


def build_dead_letter(msg, rejection: Rejection, consumer_group: str) -> DeadLetter:
    raw = msg.value() or b""
    try:
        text, b64 = raw.decode("utf-8"), None
    except UnicodeDecodeError:
        text, b64 = None, base64.b64encode(raw).decode("ascii")
    key = msg.key()
    timestamp_type, timestamp_ms = msg.timestamp() if hasattr(msg, "timestamp") else (0, None)
    record = {
        "dlq_schema_version": DLQ_SCHEMA_VERSION,
        "failure_reason": rejection.reason,
        "failure_details": list(rejection.details),
        "failed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "consumer_group": consumer_group,
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "source_timestamp_ms": timestamp_ms if timestamp_type else None,
        "source_key": key.decode("utf-8", "replace") if key is not None else None,
        "original_payload_utf8": text,
        "original_payload_base64": b64,
    }
    headers = [("dlq.reason", rejection.reason.encode()),
               ("dlq.source_topic", msg.topic().encode()),
               ("dlq.source_partition", str(msg.partition()).encode()),
               ("dlq.source_offset", str(msg.offset()).encode())]
    return DeadLetter(key, json.dumps(record, ensure_ascii=False).encode("utf-8"), headers,
                      msg.partition(), msg.offset())


class DeadLetterPublisher:
    def __init__(self, topic: str, producer_config: dict, flush_timeout_s: float,
                 producer_factory: Callable[[dict], object] | None = None) -> None:
        if producer_factory is None:
            from confluent_kafka import Producer
            producer_factory = Producer
        self.topic, self.flush_timeout_s = topic, flush_timeout_s
        self.producer = producer_factory(dict(producer_config))

    def publish(self, letters: list[DeadLetter]) -> list[DeadLetter]:
        """Publish and wait for confirmation. Returns the letters that were NOT confirmed."""
        confirmed: set[int] = set()
        errors: dict[int, str] = {}

        def on_delivery(index):
            def callback(err, _msg):
                if err is None:
                    confirmed.add(index)
                else:
                    errors[index] = str(err)
            return callback

        for i, letter in enumerate(letters):
            try:
                self.producer.produce(self.topic, key=letter.key, value=letter.value, headers=letter.headers,
                                      on_delivery=on_delivery(i))
            except Exception as exc:   # BufferError, KafkaException, ...: not published
                errors[i] = f"{type(exc).__name__}: {exc}"
            self.producer.poll(0)
        self.producer.flush(self.flush_timeout_s)
        self.last_errors = errors
        return [letter for i, letter in enumerate(letters) if i not in confirmed]

    def flush(self, timeout: float) -> int:
        return self.producer.flush(timeout)
