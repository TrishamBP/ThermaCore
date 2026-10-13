"""Facility layout: facility -> room -> aisle -> rack -> server (+ cooling_unit under a room).

Stable identifier contract with the 3D frontend:
  - every object has a unique, stable `id` (e.g. srv-013, rack-21); Three.js meshes carry it in userData;
  - objects linked to telemetry also carry `asset_id` (the telemetry system's id, e.g. SIM-NA-013);
  - clicks are resolved by id, never by screen or mesh position.

The layout version is a hash of the file content plus the declared revision, so any edit changes it;
LayoutProvider reloads when the file's modification time changes (cache invalidation).
"""

import hashlib
import math
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml

from data_engineering.paths import REPO_ROOT

DEFAULT_LAYOUT_PATH = REPO_ROOT / "config" / "digital_twin_layout.yaml"
OBJECT_TYPES = ("facility", "room", "aisle", "rack", "server", "cooling_unit")
ALLOWED_PARENT = {"facility": (None,), "room": ("facility",), "aisle": ("room",), "rack": ("aisle", "room"),
                  "server": ("rack",), "cooling_unit": ("room", "aisle")}
OVERLAY_BAND_TYPES = ("temperature", "workload", "airflow", "energy")


class LayoutError(ValueError):
    pass


class LayoutNotConfiguredError(LayoutError):
    pass


@dataclass(frozen=True)
class LayoutObject:
    id: str
    type: str
    name: str
    position: tuple[float, float, float]
    dimensions: tuple[float, float, float]
    parent_id: str | None = None
    asset_id: str | None = None
    rotation: tuple[float, float, float] = (0.0, 0.0, 0.0)
    metadata: dict = field(default_factory=dict)

    def to_dict(self, version: str, telemetry_available: bool) -> dict:
        return {"id": self.id, "asset_id": self.asset_id, "parent_id": self.parent_id, "name": self.name,
                "type": self.type, "position": dict(zip("xyz", self.position)),
                "rotation": dict(zip("xyz", self.rotation)),
                "dimensions": dict(zip(("width", "height", "depth"), self.dimensions)), "metadata": self.metadata,
                "telemetry_available": telemetry_available, "layout_version": version}


@dataclass
class Layout:
    version: str
    revision: str
    demonstration: bool
    units: str
    objects: dict[str, LayoutObject]
    overlay_bands: dict[str, list[dict]]
    source: str
    loaded_at: datetime

    def children(self, object_id: str) -> list[LayoutObject]:
        return [o for o in self.objects.values() if o.parent_id == object_id]

    def descendants(self, object_id: str) -> list[LayoutObject]:
        out, stack = [], [object_id]
        while stack:
            for c in self.children(stack.pop()):
                out.append(c)
                stack.append(c.id)
        return out

    def ancestors(self, object_id: str) -> list[LayoutObject]:
        out, o = [], self.objects[object_id]
        while o.parent_id:
            o = self.objects[o.parent_id]
            out.append(o)
        return out

    def by_asset(self) -> dict[str, LayoutObject]:
        return {o.asset_id: o for o in self.objects.values() if o.asset_id}

    def room_of(self, object_id: str) -> str | None:
        o = self.objects[object_id]
        if o.type == "room":
            return o.id
        return next((a.id for a in self.ancestors(object_id) if a.type == "room"), None)


def _vec(v, name, positive=False):
    if not isinstance(v, (list, tuple)) or len(v) != 3 or not all(isinstance(x, (int, float)) and
                                                                  not isinstance(x, bool) and math.isfinite(x)
                                                                  for x in v):
        raise LayoutError(f"{name} must be three finite numbers")
    if positive and any(x <= 0 for x in v):
        raise LayoutError(f"{name} must be positive")
    return tuple(float(x) for x in v)


def parse_layout(text: str, source: str = "<string>") -> Layout:
    try:
        raw = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise LayoutError(f"invalid YAML: {exc}") from exc
    meta, items = raw.get("layout") or {}, raw.get("objects") or []
    if not isinstance(items, list) or not items:
        raise LayoutError("layout has no objects")
    objects, problems, asset_ids = {}, [], {}
    for n, it in enumerate(items):
        try:
            oid = it["id"]
            if not isinstance(oid, str) or not oid:
                raise LayoutError("id must be a non-empty string")
            if oid in objects:
                raise LayoutError(f"duplicate object id {oid!r}")
            if it.get("type") not in OBJECT_TYPES:
                raise LayoutError(f"{oid}: invalid type {it.get('type')!r} (allowed: {', '.join(OBJECT_TYPES)})")
            obj = LayoutObject(id=oid, type=it["type"], name=str(it.get("name") or oid),
                               position=_vec(it.get("position"), f"{oid}.position"),
                               dimensions=_vec(it.get("dimensions"), f"{oid}.dimensions", positive=True),
                               parent_id=it.get("parent_id"), asset_id=it.get("asset_id"),
                               rotation=_vec(it.get("rotation", [0, 0, 0]), f"{oid}.rotation"),
                               metadata=dict(it.get("metadata") or {}))
            if obj.asset_id is not None:
                if obj.asset_id in asset_ids:
                    raise LayoutError(f"{oid}: asset_id {obj.asset_id!r} already mapped to {asset_ids[obj.asset_id]}")
                asset_ids[obj.asset_id] = oid
            objects[oid] = obj
        except (KeyError, TypeError) as exc:
            problems.append(f"object #{n}: malformed ({exc})")
        except LayoutError as exc:
            problems.append(str(exc))
    for o in objects.values():
        if o.parent_id is not None and o.parent_id not in objects:
            problems.append(f"{o.id}: parent {o.parent_id!r} does not exist")
        elif (objects[o.parent_id].type if o.parent_id else None) not in ALLOWED_PARENT[o.type]:
            problems.append(f"{o.id}: a {o.type} cannot be placed under "
                            f"{objects[o.parent_id].type if o.parent_id else 'nothing'}")
    for o in objects.values():                       # cycles
        seen, cur = set(), o
        while cur.parent_id and cur.parent_id in objects:
            if cur.id in seen:
                problems.append(f"{o.id}: cyclic parent relationship")
                break
            seen.add(cur.id)
            cur = objects[cur.parent_id]
    bands = raw.get("overlay_bands") or {}
    for name, b in bands.items():
        if name not in OVERLAY_BAND_TYPES or not isinstance(b, list) or not b or b[-1].get("max") is not None or \
                any(not isinstance(x.get("max"), (int, float)) for x in b[:-1]) or \
                [x["max"] for x in b[:-1]] != sorted(x["max"] for x in b[:-1]):
            problems.append(f"overlay_bands.{name}: ascending 'max' values with a final open band (max: null)")
    if problems:
        raise LayoutError("; ".join(dict.fromkeys(problems)))
    revision = str(meta.get("revision", "unversioned"))
    version = f"{revision}-{hashlib.sha256(text.encode()).hexdigest()[:10]}"
    return Layout(version=version, revision=revision, demonstration=bool(meta.get("demonstration", False)),
                  units=str(meta.get("units", "m")), objects=objects, overlay_bands=bands, source=source,
                  loaded_at=datetime.now(timezone.utc))


class LayoutProvider:
    """Caches the parsed layout; reloads when the file changes (mtime/size)."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path or os.environ.get("DIGITAL_TWIN_LAYOUT") or DEFAULT_LAYOUT_PATH)
        self._cache: tuple[tuple, Layout] | None = None
        self._lock = threading.Lock()

    def get(self) -> Layout:
        try:
            st = self.path.stat()
        except FileNotFoundError as exc:
            raise LayoutNotConfiguredError(f"no digital twin layout at {self.path.name}") from exc
        key = (st.st_mtime_ns, st.st_size)
        with self._lock:
            if self._cache is None or self._cache[0] != key:
                self._cache = (key, parse_layout(self.path.read_text(encoding="utf-8"), self.path.name))
            return self._cache[1]
