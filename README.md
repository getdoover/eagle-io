# eagle.io

A Doover processor that follows points in [eagle.io](https://eagle.io) and
brings their new readings into Doover. It does no interpretation itself: every
15 minutes it asks eagle.io what has arrived since the last poll, then

- records the points you give a **Tag** as tags of this app (`tag_values`):
  the newest reading as the current value, every reading in history at its own
  time;
- relays everything to a channel as JTS (JSON Time Series), eagle.io's own
  format, where a **JTS Renderer** (from
  [mqtt-bridge](https://github.com/getdoover/mqtt-bridge)) can turn the columns
  into tags and a UI, the same way it does for eagle.io's MQTT feed.

Use either or both.

```
eagle.io  --(HTTP poll, JTS)-->  eagle_io  --(channel "eagle_io")-->  jts_renderer  -->  tags
```

## Why polling

eagle.io has no webhook or push API for data; its notifications go only to
email and SMS. Many eagle.io sources are dataloggers that upload hours of
records in one call, so the app asks for records **modified** since the last
poll (`/historic?timeQuery=MODIFIED`), not records time-stamped since then.
Late uploads arrive in the next poll, at their own timestamps.

One poll is three requests or fewer: two to list the location's points, one
for their data (one per 100 points). eagle.io allows 350 requests per 5
minutes per IP address.

## Configuration

| Setting | |
| --- | --- |
| **API Key** | An eagle.io API key that can read the points. |
| **Location or Data Source ID** | Follow every point under this location or source, under its eagle.io name. |
| **Points** | Points to follow by id, each with an optional **Name** and **Tag**. **Name** renames the JTS column, and so the tag the renderer writes (`River Level` → `river_level`). **Tag** records the point as a tag of this app (e.g. `river_level`). Listing a point already under the location just applies these settings. |
| **Target Channel** | Where JTS batches go (default `eagle_io`). Set the JTS Renderer's subscription and Source Channel to the same name. Leave blank to only record tags. |
| **First Poll (hours)** *(advanced)* | How far back a new install reads (default 24). |
| **Schedule** *(advanced)* | Default every 15 minutes. |

Node ids are the 24-character ids in the eagle.io URL of a location, source
or point, or from `GET /api/v1/nodes`.

Each message is `{"source": "eagle.io", "payload": <JTS document>}`. The JTS
Renderer unwraps this envelope as it does the MQTT poller's.

## Not in scope

Long history: the app reads only what is new. Backfill a new site once,
outside the app, by posting JTS from `/historic` (time query `TIMESTAMP`) to
the same channel.

## Tags

Each mapped point's tag holds its newest reading. Its history goes into
`tag_values` at each reading's own time, one message per time with every
mapped tag read then. A late upload of older readings goes into history without replacing a newer
current value (`tag_times` tracks the time each tag holds).

The app's own bookkeeping tags: `modified_since` (end of the last poll's window, the cursor), `last_poll_at`,
`points`, `records_relayed` and `last_error`. A failed poll leaves the cursor
alone, so the next poll retries the same window.

## Development

```sh
uv sync
uv run pytest
uv run export-config   # regenerate doover_config.json after changing app_config.py
```

Pushing to `main` publishes the app through the shared Doover App workflow.
