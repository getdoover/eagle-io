"""AWS Lambda entry point for the eagle.io processor."""

from typing import Any

from pydoover.processor import run_app

from .application import EagleIo


def handler(event: dict[str, Any], context: Any) -> None:
    run_app(EagleIo(), event, context)
