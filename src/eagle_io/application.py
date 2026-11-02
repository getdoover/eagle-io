"""Poll eagle.io for newly arrived records and relay them to a Doover channel as JTS.

The app knows nothing about what the points measure. It finds the points,
fetches whatever eagle.io received since the last poll, renames the columns
as configured, and writes the JTS document to a channel. A JTS Renderer
subscribed to that channel turns it into tags and a UI, exactly as it does for
eagle.io's MQTT feed.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from pydoover.models import DeploymentEvent, ManualInvokeEvent, ScheduleEvent
from pydoover.processor import Application

from . import api
from .app_config import EagleIoConfig
from .app_tags import EagleIoTags

log = logging.getLogger(__name__)

# Each poll re-reads a little of the previous window, so a record committed
# while the last poll was running isn't missed. The renderer skips records it
# has already written, so the overlap costs nothing downstream.
OVERLAP = timedelta(minutes=5)


def parse_time(raw: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")) if raw else None
    except ValueError:
        return None


def resolve_points(found: list[dict], listed: list[tuple[str, str | None]]) -> dict[str, str | None]:
    """Point id -> configured name (None keeps eagle.io's), discovered first, then listed."""
    points: dict[str, str | None] = {n["_id"]: None for n in found}
    for point_id, name in listed:
        if point_id:
            points[point_id] = name or None
    return points


def record_count(doc: dict) -> int:
    return len(doc.get("data") or [])


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

        now = datetime.now(timezone.utc).replace(microsecond=0)
        since = parse_time(self.tags.modified_since.value)
        start = since - OVERLAP if since else now - timedelta(hours=self.config.first_poll_hours.value or 0)

        try:
            points = await asyncio.to_thread(self._points, base_url, api_key)
            if not points:
                await self.tags.last_error.set("No points found: set a location or list points")
                return
            docs = await asyncio.to_thread(api.modified, base_url, api_key, list(points), start, now)
        except api.EagleIoError as e:
            # Leave the window where it was, so the next poll retries it.
            log.error("eagle.io poll failed: %s", e)
            await self.tags.last_error.set(str(e))
            return

        names = {pid: name for pid, name in points.items() if name}
        channel = self.config.target_channel.value or "eagle_io"
        relayed = 0
        for doc in docs:
            if not record_count(doc):
                continue
            api.rename_columns(doc, names)
            await self.api.create_message(channel, {"source": "eagle.io", "payload": doc})
            relayed += record_count(doc)

        await self.tags.modified_since.set(api.iso(now))
        await self.tags.last_poll_at.set(int(now.timestamp() * 1000))
        await self.tags.points.set(len(points))
        await self.tags.last_error.set("")
        if relayed:
            await self.tags.records_relayed.increment(relayed)
        log.info(
            "%s point(s), %s record(s) modified %s..%s -> %s",
            len(points), relayed, api.iso(start), api.iso(now), channel,
        )

    def _points(self, base_url: str, api_key: str) -> dict[str, str | None]:
        node_id = (self.config.node_id.value or "").strip()
        found = api.points_under(base_url, api_key, node_id) if node_id else []
        listed = [
            ((p.point_id.value or "").strip(), (p.name.value or "").strip())
            for p in self.config.points.elements
        ]
        return resolve_points(found, listed)
