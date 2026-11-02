"""Poll eagle.io for newly arrived records and pass them on.

The app knows nothing about what the points measure. It finds the points,
fetches whatever eagle.io received since the last poll, and then:

- records each point that has a **Tag** as a tag of this app: the newest
  reading as its current value, and every reading in its history at the
  reading's own time;
- relays the whole JTS document, columns renamed as configured, to a channel
  where a JTS Renderer can turn it into tags and a UI, exactly as it does for
  eagle.io's MQTT feed.
"""

import asyncio
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pydoover.models import DeploymentEvent, ManualInvokeEvent, ScheduleEvent
from pydoover.models.data import BatchMutationItem
from pydoover.processor import Application

from . import api
from .app_config import EagleIoConfig
from .app_tags import EagleIoTags

log = logging.getLogger(__name__)

# Each poll re-reads a little of the previous window, so a record committed
# while the last poll was running isn't missed. Re-writing a reading is
# harmless: its history message has a fixed id (see history_id), so it is
# overwritten rather than duplicated, and the renderer skips records it has
# already written.
OVERLAP = timedelta(minutes=5)
DOOVER_EPOCH_MS = 1735689600000
# Snowflake fields reserved for this app's history messages, apart from the
# platform's own ids and the FWIN CSV import's (region 15, instance 1023).
HISTORY_REGION, HISTORY_INSTANCE, MESSAGE_TYPE = 15, 1022, 2
MAX_SLOTS = 16  # the id's 4-bit sequence field holds the tag's slot
BATCH = 50
_UNSAFE = re.compile(r"[^0-9a-zA-Z_]+")


@dataclass(frozen=True)
class Point:
    name: str | None = None  # JTS column name; None keeps eagle.io's
    tag: str | None = None  # tag key to record the point under, if any
    time_tag: str | None = None  # tag holding the time (ms) of the tag's newest reading


def parse_time(raw: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw) if raw else None
    except ValueError:
        return None


def tag_key(raw: str) -> str:
    """A usable tag key from what was typed: 'River Level' -> river_level."""
    return _UNSAFE.sub("_", raw.strip()).strip("_").lower()


def resolve_points(
    found: list[dict], listed: list[tuple[str, str, str, str]]
) -> dict[str, Point]:
    """Point id -> its settings, discovered points first, then listed ones."""
    points: dict[str, Point] = {n["_id"]: Point() for n in found}
    for point_id, name, tag, time_tag in listed:
        if point_id:
            tag = tag_key(tag) or None
            # A time tag only means something alongside a value tag.
            points[point_id] = Point(
                name or None, tag, (tag and tag_key(time_tag)) or None
            )
    return points


def record_count(doc: dict) -> int:
    return len(doc.get("data") or [])


def tag_readings(
    docs: list[dict], tags: dict[str, str]
) -> dict[str, list[tuple[int, object]]]:
    """Tag -> [(ms since epoch, value)] for the mapped points' readings in the documents."""
    readings: dict[str, list[tuple[int, object]]] = defaultdict(list)
    for doc in docs:
        columns = ((doc.get("header") or {}).get("columns") or {}).items()
        tag_of = {key: tags[c.get("id")] for key, c in columns if c.get("id") in tags}
        if not tag_of:
            continue
        for row in doc.get("data") or []:
            ts = parse_time(row.get("ts") or "")
            if ts is None:
                continue
            for key, cell in (row.get("f") or {}).items():
                value = cell.get("v") if isinstance(cell, dict) else cell
                if key in tag_of and value is not None:
                    readings[tag_of[key]].append((int(ts.timestamp() * 1000), value))
    return readings


def history_id(ms: int, slot: int) -> int:
    """A fixed message id for one tag's reading at one time.

    eagle.io often re-reports a record (late uploads, a running total updated
    through its period, the poll overlap), and a message written again under
    the same id replaces the old one instead of adding a duplicate that would
    be counted twice in rain totals.
    """
    return (
        ((ms - DOOVER_EPOCH_MS) << 22)
        | (HISTORY_REGION << 18)
        | (HISTORY_INSTANCE << 8)
        | (MESSAGE_TYPE << 4)
        | slot
    )


def history_items(
    readings: dict[str, list[tuple[int, object]]], order: list[str]
) -> list[tuple[int, int | None, str, object]]:
    """(ms, message id, tag, value) per reading, oldest first.

    One message per tag and time, its id fixed by both. A tag's slot is its
    position in the configured points, so adding a point at the end keeps the
    others' ids; tags past the 16th get no fixed id.
    """
    items = []
    for tag, values in readings.items():
        slot = order.index(tag)
        for ms, value in values:
            items.append(
                (ms, history_id(ms, slot) if slot < MAX_SLOTS else None, tag, value)
            )
    return sorted(items, key=lambda i: (i[0], i[2]))


class EagleIo(Application):
    config_cls = EagleIoConfig
    tags_cls = EagleIoTags

    config: EagleIoConfig
    tags: EagleIoTags

    async def on_schedule(self, event: ScheduleEvent):
        await self.poll()

    async def on_deployment(self, event: DeploymentEvent):
        # Poll on deploy so a new install shows data without waiting a cycle.
        await self.poll()

    async def on_manual_invoke(self, event: ManualInvokeEvent):
        await self.poll()

    async def poll(self):
        api_key = (self.config.api_key.value or "").strip()
        if not api_key:
            await self.tags.last_error.set("No API key configured")
            return
        base_url = self.config.base_url.value or api.DEFAULT_URL

        now = datetime.now(UTC).replace(microsecond=0)
        since = parse_time(self.tags.modified_since.value)
        start = (
            since - OVERLAP
            if since
            else now - timedelta(hours=self.config.first_poll_hours.value or 0)
        )

        try:
            points = await asyncio.to_thread(self._points, base_url, api_key)
            if not points:
                await self.tags.last_error.set(
                    "No points found: set a location or list points"
                )
                return
            docs = await asyncio.to_thread(
                api.modified, base_url, api_key, list(points), start, now
            )
        except api.EagleIoError as e:
            # Leave the window where it was, so the next poll retries it.
            log.error("eagle.io poll failed: %s", e)
            await self.tags.last_error.set(str(e))
            return

        docs = [d for d in docs if record_count(d)]
        recorded = await self.record_tags(docs, points)

        channel = (self.config.target_channel.value or "").strip()
        relayed = 0
        if channel:
            names = {pid: p.name for pid, p in points.items() if p.name}
            for doc in docs:
                api.rename_columns(doc, names)
                await self.api.create_message(
                    channel, {"source": "eagle.io", "payload": doc}
                )
                relayed += record_count(doc)

        await self.tags.modified_since.set(api.iso(now))
        await self.tags.last_poll_at.set(int(now.timestamp() * 1000))
        await self.tags.points.set(len(points))
        await self.tags.last_error.set("")
        if relayed:
            await self.tags.records_relayed.increment(relayed)
        log.info(
            "%s point(s), %s record(s) modified %s..%s; %s tag reading(s) recorded, %s relayed to %s",
            len(points),
            sum(map(record_count, docs)),
            api.iso(start),
            api.iso(now),
            recorded,
            relayed,
            channel or "(no channel)",
        )

    async def record_tags(self, docs: list[dict], points: dict[str, Point]) -> int:
        """Write mapped points to tag_values: history at reading times, newest as current."""
        tags = {pid: p.tag for pid, p in points.items() if p.tag}
        if not tags:
            return 0
        readings = tag_readings(docs, tags)

        order = list(dict.fromkeys(tags.values()))
        items = [
            BatchMutationItem(
                agent_id=self.agent_id,
                channel_name="tag_values",
                data={self.app_key: {tag: value}},
                message_id=mid,
                timestamp=ms,
            )
            for ms, mid, tag, value in history_items(readings, order)
        ]
        for i in range(0, len(items), BATCH):
            await self.api.batch_create_messages(items[i : i + BATCH])

        # A late upload can hold readings older than the current value; those
        # belong in history only.
        times = dict(self.tags.tag_times.value or {})
        time_tag_of = {
            p.tag: p.time_tag for p in points.values() if p.tag and p.time_tag
        }
        stamped: dict[str, int] = {}
        for tag, values in readings.items():
            ms, value = max(values, key=lambda r: r[0])
            if ms >= times.get(tag, 0):
                await self.set_tag(tag, value)
                times[tag] = ms
                if tag in time_tag_of:
                    # Points can share a time tag (rain_time for every rain
                    # figure); it holds the newest of their times.
                    key = time_tag_of[tag]
                    stamped[key] = max(ms, stamped.get(key, 0))
        for key, ms in stamped.items():
            if ms >= (self.get_tag(key) or 0):
                await self.set_tag(key, ms)
        await self.tags.tag_times.set(times)
        return len(items)

    def _points(self, base_url: str, api_key: str) -> dict[str, Point]:
        node_id = (self.config.node_id.value or "").strip()
        found = api.points_under(base_url, api_key, node_id) if node_id else []
        listed = [
            (
                (p.point_id.value or "").strip(),
                (p.name.value or "").strip(),
                (p.tag.value or "").strip(),
                (p.time_tag.value or "").strip(),
            )
            for p in self.config.points.elements
        ]
        return resolve_points(found, listed)
