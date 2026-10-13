"""Create (if missing) and describe the telemetry topic and its dead-letter topic. Repeatable.

    uv run python -m data_engineering.streaming.topic_setup

Creates KAFKA_TOPIC (default datacenter.telemetry) and KAFKA_DLQ_TOPIC (default
datacenter.telemetry.dlq) with KAFKA_TOPIC_PARTITIONS, KAFKA_TOPIC_REPLICATION_FACTOR and
KAFKA_TOPIC_RETENTION_MS (see .env.example). Existing topics are never altered; a
partition-count mismatch is reported. --no-dlq creates only the telemetry topic.
"""

import argparse
import sys
import time

from data_engineering.logging_utils import get_logger
from data_engineering.streaming.settings import (
    ProducerSettings, SettingsError, load_consumer_settings, load_settings)

log = get_logger("topic_setup")


def ensure_topic(settings: ProducerSettings, admin=None, topic: str | None = None) -> dict:
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = admin or AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    topic = topic or settings.topic
    new_topic = NewTopic(topic, num_partitions=settings.topic_partitions,
                         replication_factor=settings.topic_replication_factor,
                         config={"retention.ms": str(settings.topic_retention_ms), "cleanup.policy": "delete"})
    created = False
    future = admin.create_topics([new_topic], request_timeout=settings.metadata_timeout_s)[topic]
    try:
        future.result()
        created = True
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    # A just-created topic can take a moment to appear in metadata: poll until it does.
    deadline = time.monotonic() + settings.metadata_timeout_s
    while True:
        metadata = admin.list_topics(topic=topic, timeout=settings.metadata_timeout_s)
        info = metadata.topics[topic]
        if info.error is None and info.partitions:
            break
        if time.monotonic() >= deadline:
            raise KafkaException(info.error or KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
        time.sleep(0.2)
    partitions = {p.id: {"leader": p.leader, "replicas": list(p.replicas)} for p in info.partitions.values()}
    return {"topic": topic, "created": created, "partitions": len(partitions),
            "expected_partitions": settings.topic_partitions, "brokers": len(metadata.brokers),
            "partition_leaders": partitions}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap-servers")
    ap.add_argument("--topic")
    ap.add_argument("--dlq-topic", help="override KAFKA_DLQ_TOPIC")
    ap.add_argument("--no-dlq", action="store_true", help="do not create the dead-letter topic")
    args = ap.parse_args(argv)
    try:
        settings = load_settings(bootstrap_servers=args.bootstrap_servers, topic=args.topic)
        dlq_topic = None if args.no_dlq else load_consumer_settings(
            bootstrap_servers=args.bootstrap_servers, topic=args.topic, dlq_topic=args.dlq_topic).dlq_topic
    except SettingsError as exc:
        log.error("configuration error", extra={"context": {"error": str(exc)}})
        return 2
    from confluent_kafka import KafkaException
    for topic in [settings.topic] + ([dlq_topic] if dlq_topic else []):
        try:
            result = ensure_topic(settings, topic=topic)
        except KafkaException as exc:
            log.error("topic setup failed", extra={"context": {
                "topic": topic, "error": str(exc), "bootstrap_servers": settings.bootstrap_servers,
                "hint": "is the broker running? `docker compose up -d kafka`"}})
            return 2
        if result["partitions"] != result["expected_partitions"]:
            log.warning("existing topic has a different partition count than configured; not altering it",
                        extra={"context": result})
        print(f"topic {result['topic']}: {'created' if result['created'] else 'already existed'}, "
              f"{result['partitions']} partitions, {result['brokers']} broker(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
