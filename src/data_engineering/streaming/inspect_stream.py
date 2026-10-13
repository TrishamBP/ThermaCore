"""Summarize kafka-console-consumer output to verify what the producer published.

This is a text filter, not a Kafka consumer: it reads `key<TAB>value` lines on stdin, as
printed by kafka-console-consumer.sh with `--property print.key=true`.

    docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic datacenter.telemetry --from-beginning \
        --timeout-ms 10000 --property print.key=true \
      | uv run python -m data_engineering.streaming.inspect_stream

Reports message counts per key and run, whether each key equals the event's asset_id,
whether event_timestamp increases per asset, duplicate event_ids, and schema violations.
Exit status 1 if any check fails.
"""

import json
import sys
from collections import Counter, defaultdict
from typing import Iterable, TextIO

from data_engineering.streaming.events import validate_event


def summarize(lines: Iterable[str]) -> dict:
    per_key = Counter()
    per_run = Counter()
    status = Counter()
    anomalous = 0
    key_mismatch = invalid = unparseable = 0
    last_ts: dict[str, str] = {}
    out_of_order = defaultdict(int)
    event_ids = Counter()
    first_ts = last_seen_ts = None
    for raw in lines:
        line = raw.rstrip("\r\n")
        if not line or line.startswith("Processed a total of"):
            continue
        key, sep, value = line.partition("\t")
        if not sep:
            unparseable += 1
            continue
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            unparseable += 1
            continue
        per_key[key] += 1
        per_run[event.get("simulation_run_id")] += 1
        status[event.get("reading_status")] += 1
        anomalous += bool(event.get("labels", {}).get("injected_anomalies"))
        event_ids[event.get("event_id")] += 1
        if key != event.get("asset_id"):
            key_mismatch += 1
        if validate_event(event):
            invalid += 1
        ts = event.get("event_timestamp", "")
        if key in last_ts and ts <= last_ts[key]:
            out_of_order[key] += 1
        last_ts[key] = ts
        first_ts = ts if first_ts is None or ts < first_ts else first_ts
        last_seen_ts = ts if last_seen_ts is None or ts > last_seen_ts else last_seen_ts
    total = sum(per_key.values())
    duplicates = sum(n - 1 for n in event_ids.values() if n > 1)
    return {
        "messages": total, "unparseable_lines": unparseable, "keys": len(per_key),
        "messages_per_key": dict(sorted(per_key.items())), "runs": dict(per_run),
        "reading_status": dict(status), "events_with_injected_anomalies": anomalous,
        "event_timestamp_range": [first_ts, last_seen_ts],
        "key_not_equal_asset_id": key_mismatch, "schema_invalid": invalid,
        "timestamps_not_increasing_per_key": dict(out_of_order), "duplicate_event_ids": duplicates,
    }


def main(stdin: TextIO = sys.stdin) -> int:
    report = summarize(stdin)
    print(json.dumps(report, indent=2))
    problems = report["key_not_equal_asset_id"] or report["schema_invalid"] or report["unparseable_lines"]
    if report["duplicate_event_ids"] or report["timestamps_not_increasing_per_key"]:
        print("note: duplicates / non-increasing timestamps are expected if the same run was "
              "published more than once (consumers must deduplicate by event_id)", file=sys.stderr)
    return 1 if problems or report["messages"] == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
