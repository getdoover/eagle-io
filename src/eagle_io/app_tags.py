"""Bookkeeping kept between polls, and the poller's health."""

from pydoover import tags


class EagleIoTags(tags.Tags):
    # End of the last successful poll's MODIFIED window (ISO 8601, UTC).
    modified_since = tags.String(default="")
    last_poll_at = tags.Number(default=None)  # ms since epoch
    points = tags.Number(default=0)  # points followed on the last poll
    records_relayed = tags.Number(default=0)  # running total
    last_error = tags.String(default="")
