import copy
import json

import pytest

from kafka_fakes import factory
from data_engineering.streaming import producer as producer_module
from data_engineering.streaming.producer import (
    EXIT_CONFIG, EXIT_OK, EXIT_PUBLISH_FAILED, BrokerError, Pacer, TelemetryPublisher, main)
from data_engineering.streaming.settings import ProducerSettings, SettingsError, load_settings
from data_engineering.streaming.sources import build_simulator, simulated_events


@pytest.fixture
def events(loaded_conn, sim_config):
    config = copy.deepcopy(sim_config)
    config["simulation"].update(asset_count=3, duration_minutes=10, missing_rate=0.0)
    return list(simulated_events(build_simulator(loaded_conn, config), "dc-test"))


def _publisher(**fake_kwargs):
    make, holder = factory(**fake_kwargs)
    pub = TelemetryPublisher(ProducerSettings(flush_timeout_s=1.0), producer_factory=make)
    return pub, holder["producer"]


# ---------- topic, key, payload ----------

def test_messages_go_to_topic_keyed_by_asset_id(events):
    pub, fake = _publisher()
    stats = pub.publish(events)
    assert (stats.attempted, stats.queued, stats.delivered, stats.failed) == (30, 30, 30, 0)
    assert stats.ok
    for msg, event in zip(fake.sent, events):
        assert msg.topic() == "datacenter.telemetry"
        assert msg.key() == event["asset_id"].encode()
        assert json.loads(msg.value()) == event
        assert ("schema_version", b"1.0") in msg.headers()


def test_producer_config_sets_explicit_delivery_semantics():
    _, fake = _publisher()
    cfg = fake.config
    assert cfg["acks"] == "all" and cfg["enable.idempotence"] is True
    assert cfg["delivery.timeout.ms"] == 120_000 and cfg["compression.type"] == "lz4"
    assert callable(cfg["error_cb"])


def test_per_asset_order_is_preserved(events):
    pub, fake = _publisher()
    pub.publish(events)
    per_key = {}
    for msg in fake.sent:
        per_key.setdefault(msg.key(), []).append(json.loads(msg.value())["slot_index"])
    assert all(slots == sorted(slots) for slots in per_key.values())


# ---------- failures ----------

def test_delivery_failures_are_counted_not_hidden(events):
    pub, fake = _publisher(fail_keys=["SIM-NA-002"])
    stats = pub.publish(events)
    assert stats.failed == 10 and stats.delivered == 20 and stats.queued == 30
    assert not stats.ok


def test_queued_is_not_delivered_until_flush(events):
    pub, fake = _publisher(stuck_keys=["SIM-NA-003"])
    stats = pub.publish(events)
    assert stats.queued == 30                         # accepted into the local buffer ...
    assert stats.undelivered == 10 and stats.delivered == 20   # ... but 10 never confirmed
    assert not stats.ok


def test_full_local_queue_waits_instead_of_dropping(events):
    pub, fake = _publisher(buffer_full_times=3)
    stats = pub.publish(events)
    assert stats.queue_full_waits == 3
    assert stats.delivered == 30 and stats.failed == 0


def test_invalid_events_are_rejected_and_never_sent(events):
    bad = copy.deepcopy(events[0])
    bad["metrics"]["cpu_utilization"] = None      # observed slot with a missing value
    pub, fake = _publisher()
    stats = pub.publish([bad, *events[1:]])
    assert stats.rejected == 1 and stats.delivered == 29
    assert all(json.loads(m.value())["event_id"] != bad["event_id"] for m in fake.sent)
    assert not stats.ok


def test_fatal_client_error_stops_publishing(events):
    from confluent_kafka import KafkaError
    pub, fake = _publisher()

    def stream():
        for i, e in enumerate(events):
            if i == 5:
                err = KafkaError(KafkaError._FATAL, "fatal", fatal=True)
                fake.config["error_cb"](err)
            yield e
    stats = pub.publish(stream())
    assert stats.attempted == 5 and pub.fatal_error is not None


# ---------- shutdown, limits ----------

def test_stop_request_flushes_everything_already_queued(events):
    pub, fake = _publisher()

    def stream():
        for i, e in enumerate(events):
            if i == 12:
                pub.request_stop()          # what the SIGINT/SIGTERM handler does
            yield e
    stats = pub.publish(stream())
    assert stats.attempted == 12 and stats.delivered == 12
    assert fake.flush_calls == 1 and len(fake) == 0


def test_max_events_limits_a_continuous_stream(loaded_conn, sim_config):
    simulator = build_simulator(loaded_conn, sim_config)
    pub, fake = _publisher()
    stats = pub.publish(simulated_events(simulator, "dc-test", continuous=True), max_events=7)
    assert stats.attempted == stats.delivered == 7


def test_flush_runs_even_if_the_source_raises(events):
    pub, fake = _publisher()

    def stream():
        yield from events[:3]
        raise RuntimeError("source broke")
    with pytest.raises(RuntimeError):
        pub.publish(stream())
    assert fake.flush_calls == 1 and pub.stats.delivered == 3


# ---------- broker / topic checks ----------

def test_check_topic_reports_partitions():
    pub, _ = _publisher(partitions=3)
    assert pub.check_topic() == 3


def test_missing_topic_is_reported():
    pub, _ = _publisher(topic="something.else")
    with pytest.raises(BrokerError, match="topic_setup"):
        pub.check_topic()


def test_unreachable_broker_is_reported():
    pub, _ = _publisher(unreachable=True)
    with pytest.raises(BrokerError, match="cannot reach Kafka"):
        pub.check_topic()


# ---------- pacing ----------

def test_pacer_fixed_rate_and_speedup():
    now = [100.0]
    rate = Pacer(rate=10, clock=lambda: now[0])
    assert rate.due_in(0, "2026-01-01T00:00:00Z") == 0
    assert rate.due_in(5, "2026-01-01T00:05:00Z") == pytest.approx(0.5)
    fast = Pacer(speedup=60, clock=lambda: now[0])
    fast.due_in(0, "2026-01-01T00:00:00Z")
    assert fast.due_in(1, "2026-01-01T00:02:00Z") == pytest.approx(2.0)   # 2 simulated min at 60x
    assert Pacer(clock=lambda: now[0]).due_in(9, "2026-01-01T09:00:00Z") == 0


# ---------- configuration ----------

def test_settings_defaults_and_env_parsing():
    s = load_settings(env={})
    assert (s.bootstrap_servers, s.topic, s.acks) == ("127.0.0.1:9092", "datacenter.telemetry", "all")
    s = load_settings(env={"KAFKA_BOOTSTRAP_SERVERS": "a:1,b:2", "KAFKA_TOPIC": "t.x",
                           "KAFKA_LINGER_MS": "5", "KAFKA_ENABLE_IDEMPOTENCE": "false", "KAFKA_ACKS": "1"})
    assert (s.bootstrap_servers, s.topic, s.linger_ms, s.enable_idempotence) == ("a:1,b:2", "t.x", 5, False)
    assert load_settings(env={"KAFKA_TOPIC": "t"}, topic="override").topic == "override"


@pytest.mark.parametrize("env", [
    {"KAFKA_BOOTSTRAP_SERVERS": "localhost"},
    {"KAFKA_BOOTSTRAP_SERVERS": "host:99999"},
    {"KAFKA_TOPIC": "bad topic"},
    {"KAFKA_ACKS": "1"},                                    # idempotence needs acks=all
    {"KAFKA_COMPRESSION_TYPE": "brotli"},
    {"KAFKA_LINGER_MS": "abc"},
    {"KAFKA_ENABLE_IDEMPOTENCE": "maybe"},
    {"KAFKA_DELIVERY_TIMEOUT_MS": "1000"},                  # < request timeout + linger
    {"PRODUCER_RATE_EVENTS_PER_S": "10", "PRODUCER_SPEEDUP": "60"},
])
def test_invalid_settings_are_rejected(env):
    with pytest.raises(SettingsError):
        load_settings(env=env)


# ---------- CLI exit codes ----------

@pytest.fixture
def cli_db(loaded_conn, tmp_path, monkeypatch):
    path = loaded_conn.execute("PRAGMA database_list").fetchone()["file"]
    monkeypatch.setenv("DATACENTER_OPS_DB_PATH", path)
    for var in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC", "PRODUCER_RATE_EVENTS_PER_S", "PRODUCER_SPEEDUP"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(producer_module, "_install_signal_handlers", lambda publisher: None)
    return path


def test_cli_success_exit_code(cli_db):
    make, holder = factory()
    code = main(["simulate", "--assets", "2", "--duration-minutes", "5"], producer_factory=make)
    assert code == EXIT_OK
    assert len(holder["producer"].sent) == 10


def test_cli_delivery_failure_exit_code(cli_db):
    make, _ = factory(fail_keys=["SIM-NA-001"])
    assert main(["simulate", "--assets", "2", "--duration-minutes", "5"], producer_factory=make) == EXIT_PUBLISH_FAILED


def test_cli_unreachable_broker_exit_code(cli_db):
    make, _ = factory(unreachable=True)
    assert main(["simulate", "--assets", "1", "--duration-minutes", "1"], producer_factory=make) == EXIT_CONFIG


def test_cli_bad_settings_exit_code(cli_db):
    assert main(["simulate", "--bootstrap-servers", "nonsense"]) == EXIT_CONFIG


def test_cli_missing_database_exit_code(tmp_path, monkeypatch):
    monkeypatch.setenv("DATACENTER_OPS_DB_PATH", str(tmp_path / "absent.db"))
    assert main(["simulate", "--dry-run", "--max-events", "1"]) == EXIT_CONFIG


def test_cli_publish_stored_and_max_events(cli_db, loaded_conn, sim_config, monkeypatch, tmp_path):
    from data_engineering.simulation.generate_telemetry import run_simulation
    monkeypatch.setattr("data_engineering.simulation.generate_telemetry.SIMULATION_DIR", tmp_path / "sim")
    config = copy.deepcopy(sim_config)
    config["simulation"].update(asset_count=2, duration_minutes=5)
    run_simulation(loaded_conn, config)
    make, holder = factory()
    assert main(["publish-stored", "--max-events", "4"], producer_factory=make) == EXIT_OK
    sent = [json.loads(m.value()) for m in holder["producer"].sent]
    assert len(sent) == 4 and all(e["provenance"]["published_from"] == "stored_run" for e in sent)


def test_cli_dry_run_prints_valid_json_lines(cli_db, capsys):
    assert main(["simulate", "--assets", "2", "--duration-minutes", "2", "--dry-run"]) == EXIT_OK
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 4 and all(json.loads(line)["schema_version"] == "1.0" for line in lines)


def test_cli_dry_run_continuous_requires_max_events(cli_db):
    assert main(["simulate", "--continuous", "--dry-run"]) == EXIT_CONFIG


# ---------- stream inspection helper ----------

def test_inspect_stream_summarizes_console_consumer_output(events):
    from data_engineering.streaming.inspect_stream import summarize
    from data_engineering.streaming.events import serialize_event
    lines = [f"{e['asset_id']}\t{serialize_event(e).decode()}\n" for e in events]
    lines.append("Processed a total of 30 messages\n")
    report = summarize(lines)
    assert report["messages"] == 30 and report["keys"] == 3
    assert report["key_not_equal_asset_id"] == 0 and report["schema_invalid"] == 0
    assert report["timestamps_not_increasing_per_key"] == {} and report["duplicate_event_ids"] == 0
    replayed = summarize(lines + lines[:3])            # same events published twice
    assert replayed["duplicate_event_ids"] == 3
