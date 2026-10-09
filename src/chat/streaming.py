"""The heart of the app: turn an Anthropic message stream into our SSE events.

Anthropic stream (what the SDK yields)         Our SSE stream (what the browser gets)
-------------------------------------------    ------------------------------------------
message_start        {message: {id, model,…}}  -> message_start {turn, id, model, usage}
content_block_start  {index, content_block}    -> block_start   {turn, index, block}
content_block_delta  {index, delta}            -> block_delta   {turn, index, delta}
content_block_stop   {index}                   -> block_stop    {turn, index, block}
message_delta        {delta: {stop_reason}, …} -> message_delta {turn, stop_reason, …}
message_stop                                   -> message_stop  {turn}

Plus events that only our server produces:
  tool_result  - we ran a client-side tool and are sending the result back to Claude
  history      - messages the browser must append to its conversation history
  error        - something failed (API error, connection error, …)
  done         - the whole request is finished

One user question can need several API calls ("turns"): if Claude stops with
`tool_use` we run the tool and call the API again; if a server tool (web search)
stops with `pause_turn` we resend so Claude can continue. Every event carries
`turn` so the UI can keep blocks from different API calls apart.
"""

import logging
import os
from collections.abc import AsyncIterator
from typing import Any

import anthropic
from dotenv import load_dotenv

from .sse import format_sse
from .tools import TOOLS, run_tool

load_dotenv()
log = logging.getLogger("chat.streaming")

MODEL = os.getenv("CHAT_MODEL", "claude-opus-5-5")
MAX_TOKENS = 64000
MAX_TURNS = 8  # safety cap on tool / pause_turn round trips per user message

SYSTEM_PROMPT = (
    "You are a helpful assistant in a demo chat app. Use markdown where it helps. "
    "Use the calculator tool for arithmetic and get_current_time for the date or time when "
    "those tools are available."
)

# Refusal fallbacks: if the model declines on safety grounds, the API reruns the
# request on a suitable fallback model inside the same call. A `fallback`
# content block marks the hand-over point in the stream.
FALLBACK_BETA = "server-side-fallback-2026-07-01"

WEB_SEARCH_TOOL = {"type": "web_search_20260209", "name": "web_search", "max_uses": 3}

# The SDK's stream helper yields the raw API events *and* convenience events it
# synthesises ("text", "thinking", "input_json", "signature", "citation", …).
# We forward only the raw ones so the browser sees exactly what the API sent.
RAW_EVENT_TYPES = {
    "message_start",
    "content_block_start",
    "content_block_delta",
    "content_block_stop",
    "message_delta",
    "message_stop",
}

client = anthropic.AsyncAnthropic()


def _dump(model: Any) -> Any:
    """Pydantic SDK object -> JSON-safe dict, using API field names (e.g. `from`)."""
    return model.model_dump(mode="json", by_alias=True, exclude_none=True)


async def stream_chat(
    messages: list[dict[str, Any]],
    *,
    effort: str = "medium",
    thinking_display: str = "summarized",
    enable_tools: bool = True,
    enable_web_search: bool = False,
) -> AsyncIterator[str]:
    """Yield SSE-formatted strings for one user message (possibly several API turns)."""
    messages = list(messages)  # we append assistant / tool_result turns as we go
    tools: list[dict[str, Any]] = []
    if enable_tools:
        tools += TOOLS
    if enable_web_search:
        tools.append(WEB_SEARCH_TOOL)

    try:
        for turn in range(MAX_TURNS):
            request: dict[str, Any] = dict(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                messages=messages,
                # Adaptive thinking: Claude decides how much to think. "summarized"
                # streams a readable summary; "omitted" streams thinking blocks with
                # empty text (you only see a signature).
                thinking={"type": "adaptive", "display": thinking_display},
                output_config={"effort": effort},
                betas=[FALLBACK_BETA],
                fallbacks="default",
            )
            if tools:
                request["tools"] = tools

            log.info("turn %d: calling %s with %d messages", turn, MODEL, len(messages))

            # ---- 1. Open the stream and forward every raw event ----------------
            async with client.beta.messages.stream(**request) as stream:
                async for event in stream:
                    if event.type not in RAW_EVENT_TYPES:
                        continue
                    yield _translate(event, turn)
                final = await stream.get_final_message()

            # ---- 2. Record the assistant turn ---------------------------------
            # Send back the *full* content (thinking blocks + signatures, tool_use,
            # server tool results, fallback markers), unchanged. The API needs the
            # thinking blocks on later turns, and the history must be append-only.
            assistant_msg = {"role": "assistant", "content": [_dump(b) for b in final.content]}
            messages.append(assistant_msg)
            new_messages: list[dict[str, Any]] = [assistant_msg]

            # ---- 3. Decide whether another API call is needed -------------------
            if final.stop_reason == "pause_turn":
                # A server tool (web search) hit its iteration limit; resend as-is
                # and Claude picks up where it left off.
                yield format_sse("history", {"messages": new_messages})
                continue

            tool_uses = [b for b in final.content if b.type == "tool_use"]
            if final.stop_reason != "tool_use" or not tool_uses:
                # end_turn, max_tokens, refusal, stop_sequence: we're done.
                yield format_sse("history", {"messages": new_messages})
                break

            # ---- 4. Run client-side tools and loop ------------------------------
            tool_results = []
            for block in tool_uses:
                content, is_error = run_tool(block.name, block.input)
                result = {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": content,
                    "is_error": is_error,
                }
                tool_results.append(result)
                yield format_sse("tool_result", {"turn": turn, "name": block.name, **result})
            # All results for one assistant turn go back in a single user message.
            tool_msg = {"role": "user", "content": tool_results}
            messages.append(tool_msg)
            new_messages.append(tool_msg)
            yield format_sse("history", {"messages": new_messages})
        else:
            yield format_sse("error", {"type": "max_turns", "message": f"Stopped after {MAX_TURNS} turns"})

    # Most specific first: rate limits and server errors are retryable, 4xx are not.
    except anthropic.RateLimitError as e:
        yield format_sse("error", {"type": "rate_limit", "status": e.status_code, "message": e.message})
    except anthropic.APIStatusError as e:
        yield format_sse("error", {"type": "api_error", "status": e.status_code, "message": e.message})
    except anthropic.APIConnectionError as e:
        yield format_sse("error", {"type": "connection_error", "message": str(e)})
    except ValueError as e:
        # The SDK raises ValueError from the iterator if streamed tool JSON is unparseable.
        yield format_sse("error", {"type": "invalid_stream", "message": str(e)})

    yield format_sse("done", {})


def _translate(event: Any, turn: int) -> str:
    """Map one raw Anthropic stream event to one of our SSE events."""
    match event.type:
        case "message_start":
            msg = event.message
            return format_sse(
                "message_start",
                {"turn": turn, "id": msg.id, "model": msg.model, "usage": _dump(msg.usage)},
            )
        case "content_block_start":
            # `block` has the type (text, thinking, redacted_thinking, tool_use,
            # server_tool_use, web_search_tool_result, fallback, …) and any
            # fields known up front (tool name/id, a complete search result, …).
            return format_sse(
                "block_start",
                {"turn": turn, "index": event.index, "block": _dump(event.content_block)},
            )
        case "content_block_delta":
            # delta.type: text_delta | thinking_delta | signature_delta |
            #             input_json_delta | citations_delta
            return format_sse(
                "block_delta",
                {"turn": turn, "index": event.index, "delta": _dump(event.delta)},
            )
        case "content_block_stop":
            # The SDK helper attaches the fully accumulated block here (e.g. the
            # parsed tool input), which saves the browser from re-parsing JSON.
            return format_sse(
                "block_stop",
                {"turn": turn, "index": event.index, "block": _dump(event.content_block)},
            )
        case "message_delta":
            return format_sse(
                "message_delta",
                {
                    "turn": turn,
                    "stop_reason": event.delta.stop_reason,
                    "stop_details": _dump(event.delta.stop_details) if event.delta.stop_details else None,
                    "usage": _dump(event.usage),
                },
            )
        case "message_stop":
            return format_sse("message_stop", {"turn": turn})
    raise AssertionError(f"unexpected event type {event.type}")
