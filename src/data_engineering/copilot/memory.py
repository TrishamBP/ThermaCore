"""Bounded, in-process conversation memory (per API process; lost on restart by design).

At most max_conversations conversations (least recently used evicted), max_turns turns each, and a TTL.
Only the interpretation and a short answer summary are kept, never raw telemetry.
"""

import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field


@dataclass
class Turn:
    question: str
    asset_ids: list[str]
    all_assets: bool
    start_time: str | None
    end_time: str | None
    intents: list[str]
    answer_summary: str


@dataclass
class Conversation:
    conversation_id: str
    turns: list[Turn] = field(default_factory=list)
    updated: float = field(default_factory=time.monotonic)


class ConversationStore:
    def __init__(self, max_conversations: int = 200, max_turns: int = 10, ttl_s: float = 3600,
                 clock=time.monotonic) -> None:
        self.max_conversations, self.max_turns, self.ttl_s, self.clock = max_conversations, max_turns, ttl_s, clock
        self._items: OrderedDict[str, Conversation] = OrderedDict()
        self._lock = threading.Lock()

    def _expire(self) -> None:
        now = self.clock()
        for cid in [c for c, v in self._items.items() if now - v.updated > self.ttl_s]:
            del self._items[cid]

    def get(self, conversation_id: str | None) -> Conversation | None:
        with self._lock:
            self._expire()
            conv = self._items.get(conversation_id) if conversation_id else None
            if conv is not None:
                self._items.move_to_end(conversation_id)
            return conv

    def append(self, conversation_id: str | None, turn: Turn) -> str:
        with self._lock:
            self._expire()
            cid = conversation_id if conversation_id in self._items else f"conv-{uuid.uuid4().hex[:20]}"
            conv = self._items.setdefault(cid, Conversation(cid))
            conv.turns = [*conv.turns, turn][-self.max_turns:]
            conv.updated = self.clock()
            self._items.move_to_end(cid)
            while len(self._items) > self.max_conversations:
                self._items.popitem(last=False)
            return cid

    def __len__(self) -> int:
        return len(self._items)
