"""Server-Sent Events wire format.

An SSE stream is just a long-lived HTTP response with
`Content-Type: text/event-stream`. Each event is a few `field: value` lines
terminated by a blank line:

    event: block_delta
    data: {"turn": 0, "index": 1, "delta": {"type": "text_delta", "text": "Hi"}}

The browser (or any client) splits the byte stream on blank lines and reads the
`event` and `data` fields. We always put a single line of JSON in `data`.
"""

import json
from typing import Any


def format_sse(event: str, data: Any) -> str:
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {event}\ndata: {payload}\n\n"
