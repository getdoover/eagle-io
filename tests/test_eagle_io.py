"""Tests driven by a made-up /historic?timeQuery=MODIFIED response in eagle.io's JTS shape."""

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from eagle_io import api
from eagle_io.application import OVERLAP, EagleIo, Point, resolve_points, tag_key

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "historic_modified.json").read_text()
)
LEVEL = "000000000000000000000002"
RAIN = "000000000000000000000003"


def test_rename_columns_matches_on_point_id():
    doc = api.rename_columns(copy.deepcopy(FIXTURE), {LEVEL: "River Level"})
    names = {c["id"]: c["name"] for c in doc["header"]["columns"].values()}
    assert names[LEVEL] == "River Level"
    assert names[RAIN] == "Rainfall Total"  # untouched


def test_resolve_points_listed_names_win_and_keep_order():
    found = [{"_id": LEVEL}, {"_id": RAIN}]
    points = resolve_points(
        found,
        [(RAIN, "Rain Counter", "Rain Total"), ("extra", "", ""), ("", "ignored", "x")],
    )
    assert list(points) == [LEVEL, RAIN, "extra"]
    assert points == {
        LEVEL: Point(),
        RAIN: Point("Rain Counter", "rain_total"),
        "extra": Point(),
    }


def test_tag_key_cleans_what_was_typed():
    assert tag_key(" River Level (m) ") == "river_level_m"
    assert tag_key("rain_since_9am") == "rain_since_9am"


def test_points_under_walks_sources(monkeypatch):
    tree = {
        "loc": [
            {"_id": "src", "_class": "io.eagle.models.node.source.data.CampbellPakBus"},
            {"_id": "dash", "_class": "io.eagle.models.node.Dashboard"},
            {"_id": "p0", "_class": "io.eagle.models.node.point.NumberPoint"},
        ],
        "src": [
            {"_id": "p1", "_class": "io.eagle.models.node.point.NumberPoint"},
            {
                "_id": "p2",
                "_class": "io.eagle.models.node.point.process.NumberProcessPoint",
            },
            {"_id": "chart", "_class": "io.eagle.models.node.chart.HistoricChart"},
        ],
    }
    monkeypatch.setattr(api, "children", lambda base, key, parent: tree.get(parent, []))
    assert [n["_id"] for n in api.points_under("u", "k", "loc")] == ["p1", "p2", "p0"]


def test_modified_chunks_ids_and_asks_for_modified(monkeypatch):
    calls = []
    monkeypatch.setattr(
        api, "_get", lambda base, key, path, params: calls.append((path, params)) or {}
    )
    ids = [f"{i:024d}" for i in range(api.IDS_PER_REQUEST + 1)]
    start = datetime(2026, 11, 1, tzinfo=UTC)
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


def fake_app(
    monkeypatch, modified_since="", fail=False, channel="eagle_io", tag_times=None
):
    sent = []
    history = []
    current = {}

    async def create_message(channel, data, timestamp=None):
        (history if channel == "tag_values" else sent).append(
            (timestamp, data) if channel == "tag_values" else (channel, data)
        )

    async def set_tag(key, value):
        current[key] = value

    def modified(base, key, ids, start, end):
        if fail:
            raise api.EagleIoError("HTTP 429 on historic: rate limited", 429)
        fake.window = (start, end)
        return [copy.deepcopy(FIXTURE), {"header": {"columns": {}}, "data": []}]

    monkeypatch.setattr(api, "modified", modified)
    value = lambda v: SimpleNamespace(value=v)
    fake = SimpleNamespace(
        config=SimpleNamespace(
            api_key=value("key"),
            base_url=value(api.DEFAULT_URL),
            first_poll_hours=value(24.0),
            target_channel=value(channel),
        ),
        tags=SimpleNamespace(
            modified_since=FakeTag(modified_since),
            last_poll_at=FakeTag(None),
            points=FakeTag(0),
            records_relayed=FakeTag(0),
            last_error=FakeTag(""),
            tag_times=FakeTag(tag_times or {}),
        ),
        api=SimpleNamespace(create_message=create_message),
        app_key="eagle_io_1",
        set_tag=set_tag,
        _points=lambda base, key: {
            LEVEL: Point("River Level", "river_level"),
            RAIN: Point(None, "rain_total"),
        },
        sent=sent,
        history=history,
        current=current,
    )
    fake.record_tags = lambda docs, points: EagleIo.record_tags(fake, docs, points)
    return fake


async def test_poll_relays_renamed_jts_and_advances_cursor(monkeypatch):
    app = fake_app(monkeypatch)
    await EagleIo.poll(app)

    assert len(app.sent) == 1  # the empty document is not sent
    channel, message = app.sent[0]
    assert channel == "eagle_io" and message["source"] == "eagle.io"
    names = {
        c["id"]: c["name"] for c in message["payload"]["header"]["columns"].values()
    }
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
    assert start == datetime(2026, 11, 2, tzinfo=UTC) - OVERLAP


async def test_failed_poll_keeps_cursor(monkeypatch):
    app = fake_app(monkeypatch, modified_since="2026-11-02T00:00:00.000Z", fail=True)
    await EagleIo.poll(app)
    assert app.tags.modified_since.value == "2026-11-02T00:00:00.000Z"
    assert "429" in app.tags.last_error.value
    assert app.sent == []


def ms(iso):
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


async def test_mapped_points_become_tags_with_history(monkeypatch):
    app = fake_app(monkeypatch)
    await EagleIo.poll(app)

    # Newest reading of each mapped point is its current value.
    assert app.current == {"river_level": 1.251, "rain_total": 100.4}
    assert app.tags.tag_times.value == {
        "river_level": ms("2026-01-01T01:00:00Z"),
        "rain_total": ms("2026-01-01T01:00:00Z"),
    }
    # Every reading lands in history at its own time; each tag gets its own
    # millisecond so a later poll can't overwrite another tag's reading.
    assert sorted(app.history, key=lambda h: h[0]) == [
        (ms("2026-01-01T00:00:00Z"), {"eagle_io_1": {"river_level": 1.234}}),
        (ms("2026-01-01T00:00:00Z") + 1, {"eagle_io_1": {"rain_total": 100.0}}),
        (ms("2026-01-01T00:30:00Z"), {"eagle_io_1": {"river_level": 1.24}}),
        (ms("2026-01-01T01:00:00Z"), {"eagle_io_1": {"river_level": 1.251}}),
        (ms("2026-01-01T01:00:00Z") + 1, {"eagle_io_1": {"rain_total": 100.4}}),
    ]
    # Unmapped points (battery, serial) are not recorded as tags.
    assert all(
        set(d["eagle_io_1"]) <= {"river_level", "rain_total"} for _, d in app.history
    )


async def test_late_older_readings_go_to_history_only(monkeypatch):
    newer = ms("2026-01-02T00:00:00Z")
    app = fake_app(monkeypatch, tag_times={"river_level": newer})
    await EagleIo.poll(app)
    assert "river_level" not in app.current
    assert app.tags.tag_times.value["river_level"] == newer
    assert any("river_level" in d["eagle_io_1"] for _, d in app.history)


async def test_blank_channel_records_tags_without_relaying(monkeypatch, caplog):
    caplog.set_level("DEBUG")
    app = fake_app(monkeypatch, channel="")
    await EagleIo.poll(app)
    assert app.sent == []
    assert app.current  # tags still recorded
    assert app.tags.records_relayed.value == 0
    # Nothing logs the JTS itself, only counts.
    assert "docType" not in caplog.text and "1.251" not in caplog.text
