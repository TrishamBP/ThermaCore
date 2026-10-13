"""SSE live-update stream for the digital twin.

One shared poller per API process (not one per browser) reads a compact snapshot every
API_TWIN_POLL_INTERVAL_S from the existing telemetry store (the Kafka consumer keeps writing it; no second
ingestion pipeline), diffs it with the previous snapshot and fans the events out to subscribers.

  events: telemetry_update, asset_status_change, alert_created, recommendation_created, layout_changed,
          heartbeat, resync_required
  - every event has a monotonically increasing id; reconnecting clients send Last-Event-ID and get the
    missed events replayed from a bounded ring buffer (or resync_required when they are too old);
  - each subscriber has a bounded queue; when it is full the oldest event is dropped and the client gets
    resync_required (it should re-fetch /digital-twin/state);
  - events are filtered by the subscriber's asset scope; the poller stops when nobody is subscribed.
"""

import asyncio
import contextlib
import itertools
import json
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

import anyio

from app.common.logging import get_logger

log = get_logger("digital_twin.stream")


def diff_snapshots(prev: dict | None, cur: dict) -> list[tuple[str, str | None, dict]]:
    """(event_type, asset_id, payload) for everything that changed. The first snapshot yields nothing."""
    if prev is None:
        return []
    events = []
    if prev["layout_version"] != cur["layout_version"]:
        events.append(("layout_changed", None, {"layout_version": cur["layout_version"]}))
    for aid, s in cur["assets"].items():
        p = prev["assets"].get(aid)
        if p is None or s["last_telemetry_timestamp"] != p["last_telemetry_timestamp"]:
            events.append(("telemetry_update", aid, s))
        if p is not None and (s["status"], s["thermal_status"]) != (p["status"], p["thermal_status"]):
            events.append(("asset_status_change", aid, {"object_id": s["object_id"], "from": {
                "status": p["status"], "thermal_status": p["thermal_status"]}, "to": {
                "status": s["status"], "thermal_status": s["thermal_status"]}}))
    for alert_id, a in cur["alerts"].items():
        if alert_id not in prev["alerts"]:
            events.append(("alert_created", a["asset_id"], a))
    for rec_id, r in cur["recommendations"].items():
        if rec_id not in prev["recommendations"]:
            events.append(("recommendation_created", r["asset_id"], r))
    return events


@dataclass
class Subscriber:
    scope: object
    queue: asyncio.Queue
    dropped: int = 0
    id: int = field(default_factory=itertools.count().__next__)


class TwinBroadcaster:
    def __init__(self, snapshot_fn: Callable[[], dict], poll_interval_s: float = 10.0, queue_size: int = 100,
                 buffer_size: int = 500, max_subscribers: int = 50) -> None:
        self.snapshot_fn, self.poll_interval_s = snapshot_fn, poll_interval_s
        self.queue_size, self.max_subscribers = queue_size, max_subscribers
        self.buffer: deque[dict] = deque(maxlen=buffer_size)
        self.subscribers: dict[int, Subscriber] = {}
        self._ids = itertools.count(1)
        self._task: asyncio.Task | None = None
        self._last: dict | None = None

    # ------------------------------------------------------------------ events
    def _event(self, etype: str, asset_id: str | None, payload: dict) -> dict:
        return {"id": next(self._ids), "event": etype, "asset_id": asset_id,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "data": payload}

    @staticmethod
    def visible(sub: Subscriber, event: dict) -> bool:
        return event["asset_id"] is None or sub.scope.permits(event["asset_id"])

    def _offer(self, sub: Subscriber, event: dict) -> None:
        if not self.visible(sub, event):
            return
        try:
            sub.queue.put_nowait(event)
        except asyncio.QueueFull:
            sub.queue.get_nowait()                      # drop the oldest, tell the client to resync
            sub.dropped += 1
            sub.queue.put_nowait(self._event("resync_required", None, {"reason": "client too slow; events dropped"}))

    def publish(self, events: list[tuple[str, str | None, dict]]) -> None:
        for etype, aid, payload in events:
            e = self._event(etype, aid, payload)
            self.buffer.append(e)
            for sub in list(self.subscribers.values()):
                self._offer(sub, e)

    # ------------------------------------------------------------------ subscriptions
    def subscribe(self, scope, last_event_id: int | None = None) -> Subscriber:
        if len(self.subscribers) >= self.max_subscribers:
            raise OverflowError("too many stream subscribers")
        sub = Subscriber(scope, asyncio.Queue(self.queue_size))
        if last_event_id is not None:
            missed = [e for e in self.buffer if e["id"] > last_event_id]
            if (self.buffer and self.buffer[0]["id"] > last_event_id + 1) or len(missed) > self.queue_size:
                # partial replay would be misleading: the client must re-fetch /digital-twin/state
                self._offer(sub, self._event("resync_required", None, {"reason": "events since Last-Event-ID are no "
                                                                                 "longer buffered"}))
            else:
                for e in missed:
                    self._offer(sub, e)
        self.subscribers[sub.id] = sub
        if self._task is None or self._task.done():
            self._task = asyncio.get_running_loop().create_task(self._run())
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        self.subscribers.pop(sub.id, None)

    async def poll_once(self) -> None:
        started = time.monotonic()
        try:
            cur = await anyio.to_thread.run_sync(self.snapshot_fn)
        except Exception as exc:          # store down: keep the stream alive, report once per poll
            log.warning("twin snapshot failed", extra={"context": {"error_type": type(exc).__name__}})
            self.publish([("resync_required", None, {"reason": "telemetry store temporarily unavailable"})])
            return
        self.publish(diff_snapshots(self._last, cur))
        self._last = cur
        log.debug("twin poll", extra={"context": {"ms": round((time.monotonic() - started) * 1000, 1)}})

    async def _run(self) -> None:
        try:
            while self.subscribers:
                await self.poll_once()
                await asyncio.sleep(self.poll_interval_s)
        except asyncio.CancelledError:
            pass

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task


def sse(event: dict) -> str:
    return f"id: {event['id']}\nevent: {event['event']}\ndata: {json.dumps(event, default=str)}\n\n"
