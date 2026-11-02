# eagle.io

A Doover processor that follows points in [eagle.io](https://eagle.io) and
relays their new readings into Doover. It does no interpretation itself: every
15 minutes it asks eagle.io what has arrived since the last poll and writes it
to a channel as JTS (JSON Time Series), eagle.io's own format. A
**JTS Renderer** (from [mqtt-bridge](https://github.com/getdoover/mqtt-bridge))
subscribed to that channel turns the columns into tags and a UI, the same way
it does for eagle.io's MQTT feed.

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
| **Points** | Points to follow by id, optionally with a **Name**. A name renames the column, and so the tag the renderer writes (`River Level` → `river_level`). Listing a point already under the location just renames it. |
| **Target Channel** | Where batches go (default `eagle_io`). Set the JTS Renderer's subscription and Source Channel to the same name. |
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

`modified_since` (end of the last poll's window, the cursor), `last_poll_at`,
`points`, `records_relayed` and `last_error`. A failed poll leaves the cursor
alone, so the next poll retries the same window.

## Development

```sh
uv sync
uv run pytest
uv run export-config   # regenerate doover_config.json after changing app_config.py
```

Pushing to `main` publishes the app through the shared Doover App workflow.
