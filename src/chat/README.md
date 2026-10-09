# Streaming chat: Anthropic API → FastAPI → browser

A minimal chat app built to show how streaming works at every hop. The browser
has an **SSE events** panel that lists each event as it arrives.

## Run it

```bash
uv run fastapi dev src/chat/server.py --port 8001
```

Open http://127.0.0.1:8001. The server reads `ANTHROPIC_API_KEY` from the repo-root `.env`.
Set `CHAT_MODEL` to override the default model (`claude-opus-5-5`).

## Files

| File | Role |
|---|---|
| `server.py` | FastAPI app: `POST /api/chat` returns a `text/event-stream`, `/` serves the UI |
| `streaming.py` | Calls the Anthropic SDK, converts its stream events into our SSE events, and runs the tool / `pause_turn` loop |
| `sse.py` | Formats one SSE event (`event:` + `data:` + blank line) |
| `tools.py` | Two client-side tools (`calculator`, `get_current_time`) so you can watch a tool round trip |
| `static/app.js` | Sends the request, parses the SSE byte stream, and renders blocks as they grow |

## The flow

```
Browser                         FastAPI (our server)                    Anthropic API
───────                         ────────────────────                    ─────────────
POST /api/chat  ───────────────▶ stream_chat(messages)
{messages: [...full history]}     client.beta.messages.stream(...) ───▶ POST /v1/messages (stream)
                                                                  ◀─── message_start
             ◀── event: message_start                             ◀─── content_block_start {thinking}
             ◀── event: block_start  {block:{type:"thinking"}}    ◀─── content_block_delta {thinking_delta}×N
             ◀── event: block_delta  {delta:{thinking_delta}}     ◀─── content_block_delta {signature_delta}
             ◀── event: block_stop                                ◀─── content_block_stop
             ◀── event: block_start  {block:{type:"text"}}        ◀─── content_block_start {text}
             ◀── event: block_delta  {delta:{text_delta}} ×N      ◀─── content_block_delta {text_delta}×N
             ◀── event: block_stop                                ◀─── content_block_stop
             ◀── event: message_delta {stop_reason, usage}        ◀─── message_delta {stop_reason}
             ◀── event: message_stop                              ◀─── message_stop
             ◀── event: history  {messages:[assistant turn]}   (full content incl. thinking + signature)
             ◀── event: done
```

**1. Browser → server.** The Messages API is stateless, so the browser keeps the
whole conversation and POSTs all of it each time. The request uses `fetch()`
rather than `EventSource` because `EventSource` only supports GET with no body.

**2. Server → Anthropic.** `client.beta.messages.stream(...)` opens one HTTP
request whose response is itself an SSE stream. The SDK parses it into typed
events. We use the `beta` namespace only for refusal fallbacks (see below).

**3. Translating events.** The SDK helper yields the raw API events plus
convenience events that it builds itself (`text`, `thinking`, `input_json`, …).
`streaming.py` forwards only the raw ones, so the panel shows what the API
actually sent. Each event is re-encoded as one SSE event with a JSON payload.

**4. Server → browser.** `StreamingResponse` writes each `yield` to the socket
as soon as it is produced. The browser reads chunks from `res.body`, buffers until
it sees `\n\n`, and parses `event:` and `data:`. Network chunks don't line up
with events, so the buffering step is required.

**5. Rendering.** A content block is identified by `(turn, index)`:
`block_start` creates the element, `block_delta` grows it, and `block_stop`
finalises it. Text is re-rendered as markdown on every delta, because a delta
can split a token (`**bo` + `ld**`).

**6. History.** When a turn ends, the server sends a `history` event containing
the assistant message with all of its content blocks (thinking text and
signature, tool_use, server tool results). The browser appends it unchanged.

## Block and delta types you'll see

| `block.type` | Deltas | Notes |
|---|---|---|
| `thinking` | `thinking_delta`, then one `signature_delta` | Adaptive thinking. With `display: "summarized"` you get a readable summary. With `"omitted"` the text is empty and only the signature arrives. At low effort the model may not think at all. |
| `redacted_thinking` | none | Encrypted reasoning. Pass it back but don't display it. |
| `text` | `text_delta`, `citations_delta` | Citations appear when the answer is grounded in web search results or documents. |
| `tool_use` | `input_json_delta` | The tool input arrives as JSON *fragments*. `eager_input_streaming: true` streams them as they are generated. `block_stop` has the parsed input. |
| `server_tool_use` / `web_search_tool_result` / `*code_execution_tool_result` | input deltas / none | Server tools run at Anthropic; their results arrive as complete blocks. Web search with dynamic filtering also produces code-execution blocks. |
| `fallback` | none | Marks the point where a refusing model handed off to a fallback model. |

The `stop_reason` arrives in `message_delta`:

- `end_turn`: the reply is finished.
- `tool_use`: we run the tool, send a `tool_result`, and call the API again in a new turn.
- `pause_turn`: a server tool paused; we resend the history and Claude continues.
- `max_tokens`: the reply was cut off at the token limit.
- `refusal`: the model declined; `stop_details` says why.

## Things worth noticing

- **One question can take several API calls.** In a tool round trip, turn 0
  ends with `stop_reason: tool_use`. The server runs the tools and emits
  `tool_result`, then turn 1 streams the answer. Every event carries `turn` so
  the UI keeps the calls apart.
- **Thinking blocks are bound to their conversation.** They must be passed back
  unchanged, and the history must be append-only. Changing the `tools` list
  mid-conversation causes a 400 (`The block is bound to a different conversation`).
  That's why the tools and thinking toggles lock until you click **New chat**.
  Changing effort between turns is fine.
- **Cancellation goes all the way upstream.** **Stop** aborts the `fetch`.
  Starlette notices the disconnect and cancels the generator, and exiting
  `async with ...stream()` closes the connection to Anthropic, so generation
  (and billing) stops. The partial reply never reaches `history`. The next
  request then has two consecutive user messages, which the API merges.
- **Errors after the response has started** can't change the HTTP status, which
  has already been sent as 200. They arrive as an `error` event in the stream.
- **Refusal fallbacks** (`fallbacks: "default"` + beta `server-side-fallback-2026-07-01`)
  are on, so a safety decline is retried on a fallback model within the same stream.
  To turn them off, remove `betas` / `fallbacks` in `streaming.py` and switch to
  `client.messages.stream`.
- FastAPI also has a built-in `fastapi.sse.EventSourceResponse`. This app writes
  the SSE format by hand so the wire format stays visible.
