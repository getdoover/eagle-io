"""Tests driven by a made-up /historic?timeQuery=MODIFIED response in eagle.io's JTS shape."""

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from eagle_io import api
from eagle_io.application import EagleIo, OVERLAP, resolve_points

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "historic_modified.json").read_text())
LEVEL = "000000000000000000000002"
RAIN = "000000000000000000000003"


def test_rename_columns_matches_on_point_id():
    doc = api.rename_columns(copy.deepcopy(FIXTURE), {LEVEL: "River Level"})
    names = {c["id"]: c["name"] for c in doc["header"]["columns"].values()}
    assert names[LEVEL] == "River Level"
    assert names[RAIN] == "Rainfall Total"  # untouched


def test_resolve_points_listed_names_win_and_keep_order():
    found = [{"_id": LEVEL}, {"_id": RAIN}]
    points = resolve_points(found, [(RAIN, "Rain Counter"), ("extra", ""), ("", "ignored")])
    assert list(points) == [LEVEL, RAIN, "extra"]
    assert points == {LEVEL: None, RAIN: "Rain Counter", "extra": None}


def test_points_under_walks_sources(monkeypatch):
    tree = {
        "loc": [
            {"_id": "src", "_class": "io.eagle.models.node.source.data.CampbellPakBus"},
            {"_id": "dash", "_class": "io.eagle.models.node.Dashboard"},
            {"_id": "p0", "_class": "io.eagle.models.node.point.NumberPoint"},
        ],
        "src": [
            {"_id": "p1", "_class": "io.eagle.models.node.point.NumberPoint"},
            {"_id": "p2", "_class": "io.eagle.models.node.point.process.NumberProcessPoint"},
            {"_id": "chart", "_class": "io.eagle.models.node.chart.HistoricChart"},
        ],
    }
    monkeypatch.setattr(api, "children", lambda base, key, parent: tree.get(parent, []))
    assert [n["_id"] for n in api.points_under("u", "k", "loc")] == ["p1", "p2", "p0"]


def test_modified_chunks_ids_and_asks_for_modified(monkeypatch):
    calls = []
    monkeypatch.setattr(api, "_get", lambda base, key, path, params: calls.append((path, params)) or {})
    ids = [f"{i:024d}" for i in range(api.IDS_PER_REQUEST + 1)]
    start = datetime(2026, 11, 1, tzinfo=timezone.utc)
    api.modified("u", "k", ids, start, start + timedelta(hours=1))
    assert [len(p["params"].split(",")) for _, p in calls] == [api.IDS_PER_REQUEST, 1]
    assert all(p["timeQuery"] == "MODIFIED" for _, p in calls)
    assert calls[0][1]["startTime"] == "2026-11-01T00:00:00.000Z"
    assert len(api.urllib.parse.urlencode(calls[0][1])) < 4096


class FakeTag:
    def __init__(self, value):
        self.value = value

    async def set(self, value):
        self.value = value

    async def increment(self, amount):
        self.value += amount


def fake_app(monkeypatch, modified_since="", fail=False):
    sent = []

    async def create_message(channel, data):
        sent.append((channel, data))

    def modified(base, key, ids, start, end):
        if fail:
            raise api.EagleIoError("HTTP 429 on historic: rate limited", 429)
        fake.window = (start, end)
        return [copy.deepcopy(FIXTURE), {"header": {"columns": {}}, "data": []}]

    monkeypatch.setattr(api, "modified", modified)
    value = lambda v: SimpleNamespace(value=v)  # noqa: E731
    fake = SimpleNamespace(
        config=SimpleNamespace(
            api_key=value("key"),
            base_url=value(api.DEFAULT_URL),
            first_poll_hours=value(24.0),
            target_channel=value("eagle_io"),
        ),
        tags=SimpleNamespace(
            modified_since=FakeTag(modified_since),
            last_poll_at=FakeTag(None),
            points=FakeTag(0),
            records_relayed=FakeTag(0),
            last_error=FakeTag(""),
        ),
        api=SimpleNamespace(create_message=create_message),
        _points=lambda base, key: {LEVEL: "River Level", RAIN: None},
        sent=sent,
    )
    return fake


async def test_poll_relays_renamed_jts_and_advances_cursor(monkeypatch):
    app = fake_app(monkeypatch)
    await EagleIo.poll(app)

    assert len(app.sent) == 1  # the empty document is not sent
    channel, message = app.sent[0]
    assert channel == "eagle_io" and message["source"] == "eagle.io"
    names = {c["id"]: c["name"] for c in message["payload"]["header"]["columns"].values()}
    assert names[LEVEL] == "River Level"

    start, end = app.window
    assert end - start == timedelta(hours=24)  # first poll
    assert app.tags.modified_since.value == api.iso(end)
    assert app.tags.records_relayed.value == len(FIXTURE["data"])
    assert app.tags.points.value == 2
    assert app.tags.last_error.value == ""


async def test_poll_resumes_from_cursor_with_overlap(monkeypatch):
    app = fake_app(monkeypatch, modified_since="2026-11-02T00:00:00.000Z")
    await EagleIo.poll(app)
    start, _ = app.window
    assert start == datetime(2026, 11, 2, tzinfo=timezone.utc) - OVERLAP


async def test_failed_poll_keeps_cursor(monkeypatch):
    app = fake_app(monkeypatch, modified_since="2026-11-02T00:00:00.000Z", fail=True)
    await EagleIo.poll(app)
    assert app.tags.modified_since.value == "2026-11-02T00:00:00.000Z"
    assert "429" in app.tags.last_error.value
    assert app.sent == []
