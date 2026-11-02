"""The parts of the eagle.io HTTP API this app reads. Blocking; call via a thread.

eagle.io has no push or webhook for data, so new readings are found by polling
``/historic`` with ``timeQuery=MODIFIED``: that returns every record added or
changed within the window, whatever its own timestamp. Loggers often upload
hours of records in one call, so querying by record time would miss them.

Docs: https://docs.eagle.io/en/latest/api/index.html
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

DEFAULT_URL = "https://api.eagle.io/api/v1"
TIMEOUT = 30
# eagle.io rejects query strings over 4096 bytes. A point id is 24 characters
# (25 with its comma), so 100 ids leave room for the rest of the query.
IDS_PER_REQUEST = 100


class EagleIoError(Exception):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def _get(base_url: str, api_key: str, path: str, params: dict[str, str]) -> Any:
    url = f"{base_url.rstrip('/')}/{path}?{urllib.parse.urlencode(params, safe='(),:$')}"
    request = urllib.request.Request(url, headers={"X-Api-Key": api_key, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:300]
        hint = {401: "API key rejected", 403: "API key lacks access", 429: "rate limited"}.get(e.code)
        raise EagleIoError(f"HTTP {e.code} on {path}: {hint or detail}", e.code) from e
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise EagleIoError(f"{path}: {e}") from e


def is_point(node: dict) -> bool:
    return ".node.point." in node.get("_class", "")


def is_source(node: dict) -> bool:
    return ".node.source." in node.get("_class", "")


def children(base_url: str, api_key: str, parent_id: str) -> list[dict]:
    nodes = _get(
        base_url,
        api_key,
        "nodes",
        {"attr": "_id,_class,name", "filter": f"parentId($eq:{parent_id})"},
    )
    return nodes if isinstance(nodes, list) else []


def points_under(base_url: str, api_key: str, node_id: str) -> list[dict]:
    """The points under a location or data source, in eagle.io's order.

    A location holds data sources, which hold the points; a source holds its
    points directly. Points can also sit straight under a location.
    """
    found: list[dict] = []
    for node in children(base_url, api_key, node_id):
        if is_point(node):
            found.append(node)
        elif is_source(node):
            found.extend(n for n in children(base_url, api_key, node["_id"]) if is_point(n))
    return found


def iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def modified(
    base_url: str, api_key: str, point_ids: list[str], start: datetime, end: datetime
) -> list[dict]:
    """JTS documents of the records modified in [start, end], one per chunk of points."""
    docs = []
    for i in range(0, len(point_ids), IDS_PER_REQUEST):
        docs.append(
            _get(
                base_url,
                api_key,
                "historic",
                {
                    "params": ",".join(point_ids[i : i + IDS_PER_REQUEST]),
                    "startTime": iso(start),
                    "endTime": iso(end),
                    "timeQuery": "MODIFIED",
                    "format": "JSON",
                },
            )
        )
    return docs


def rename_columns(doc: dict, names: dict[str, str]) -> dict:
    """Give the JTS columns the configured names, matched on their point id.

    Consumers such as the JTS Renderer derive tag keys from column names, so
    this is what maps an eagle.io point onto a chosen tag.
    """
    for column in ((doc.get("header") or {}).get("columns") or {}).values():
        name = names.get(column.get("id"))
        if name:
            column["name"] = name
    return doc
