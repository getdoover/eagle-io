"""Installation config: which eagle.io points to follow, and where to send them."""

from pathlib import Path

from pydoover import config
from pydoover.processor import ScheduleConfig

from .api import DEFAULT_URL


class PointConfig(config.Object):
    point_id = config.String(
        "Point ID",
        name="point_id",
        description="The eagle.io point's 24-character node id.",
    )
    name = config.String(
        "Name",
        name="name",
        default=None,
        description=(
            "Column name to publish the point under, which consumers such as the "
            "JTS Renderer turn into the tag key ('River Level' -> river_level). "
            "Leave blank to keep the point's eagle.io name."
        ),
    )
    tag = config.String(
        "Tag",
        name="tag",
        default=None,
        description=(
            "Also record this point as a tag of this app (e.g. river_level): "
            "the latest reading as its current value, and every reading in its "
            "history at its own time. Leave blank to only relay it."
        ),
    )


class EagleIoConfig(config.Schema):
    api_key = config.String(
        "API Key",
        name="api_key",
        description="An eagle.io API key with read access to the points below.",
    )
    node_id = config.String(
        "Location or Data Source ID",
        name="node_id",
        default=None,
        description=(
            "Every point under this eagle.io location (or data source) is "
            "followed, under its own name. Leave blank to follow only the points "
            "listed below."
        ),
    )
    points = config.Array(
        "Points",
        name="points",
        element=PointConfig("Point"),
        description=(
            "Points to follow by id, to rename, or to record as tags. A point "
            "listed here that is also under the location above is followed "
            "once, with these settings."
        ),
    )
    target_channel = config.String(
        "Target Channel",
        name="target_channel",
        default="eagle_io",
        description=(
            "Doover channel each batch of new records is written to, as "
            '{"source": "eagle.io", "payload": <JTS>}. Point a JTS Renderer\'s '
            "subscription at it. Leave blank to only record the tags."
        ),
    )
    first_poll_hours = config.Number(
        "First Poll (hours)",
        name="first_poll_hours",
        default=24.0,
        minimum=0.0,
        advanced=True,
        description=(
            "How far back the first poll reads. Later polls carry on from the "
            "last one. Longer history is a one-off backfill, not this app's job."
        ),
    )
    base_url = config.String(
        "API URL",
        name="base_url",
        default=DEFAULT_URL,
        advanced=True,
    )
    schedule = ScheduleConfig(
        allowed_modes=["rate", "cron"], default="rate(15 minutes)", advanced=True
    )


def export() -> None:
    EagleIoConfig.export(Path(__file__).parents[2] / "doover_config.json", "eagle_io")


if __name__ == "__main__":
    export()
