"""FastAPI server.

Run from the repo root:
    uv run fastapi dev src/chat/server.py
then open http://127.0.0.1:8000
"""

import logging
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .streaming import MODEL, stream_chat

logging.basicConfig(level=logging.INFO)

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Streaming chat")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class ChatRequest(BaseModel):
    # The browser owns the conversation history and sends all of it every time
    # (the Messages API is stateless). Assistant turns are passed back verbatim,
    # including thinking blocks, so we don't model the block shapes here.
    messages: list[dict[str, Any]]
    effort: Literal["low", "medium", "high", "xhigh", "max"] = "medium"
    thinking_display: Literal["summarized", "omitted"] = "summarized"
    enable_tools: bool = True
    enable_web_search: bool = False


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/config")
async def config() -> dict[str, str]:
    return {"model": MODEL}


@app.post("/api/chat")
async def chat(req: ChatRequest) -> StreamingResponse:
    # POST (not GET) because we send a JSON body, which rules out the browser's
    # EventSource API; the frontend reads the SSE stream with fetch() instead.
    # If the client disconnects (Stop button / tab closed), Starlette cancels this
    # generator, and leaving `async with client.messages.stream(...)` closes the
    # upstream connection to Anthropic so we stop paying for tokens nobody reads.
    return StreamingResponse(
        stream_chat(
            req.messages,
            effort=req.effort,
            thinking_display=req.thinking_display,
            enable_tools=req.enable_tools,
            enable_web_search=req.enable_web_search,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # stop nginx-style proxies from buffering the stream
        },
    )
