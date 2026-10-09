// Browser side of the flow:
//   1. POST the whole conversation to /api/chat
//   2. Read the response body as a stream and split it into SSE events
//   3. Render each event: create a block on block_start, grow it on block_delta,
//      finalise it on block_stop
//   4. Append the server's `history` messages so the next request carries them

const $ = (id) => document.getElementById(id);
const els = {
  model: $("model"),
  messages: $("messages"),
  form: $("composer"),
  input: $("input"),
  send: $("send"),
  stop: $("stop"),
  effort: $("effort"),
  thinkingDisplay: $("thinking-display"),
  enableTools: $("enable-tools"),
  enableWebSearch: $("enable-web-search"),
  newChat: $("new-chat"),
  log: $("event-log"),
  eventCount: $("event-count"),
  showDeltas: $("show-deltas"),
  clearLog: $("clear-log"),
};

// Conversation history in Messages API format. Assistant turns hold the full
// content blocks (thinking + signature, tool_use, …) exactly as the server sent them.
let history = [];
let abortController = null;

fetch("/api/config").then((r) => r.json()).then((c) => (els.model.textContent = c.model));

// ---------------------------------------------------------------------------
// Sending a message and reading the SSE stream
// ---------------------------------------------------------------------------

els.form.addEventListener("submit", (e) => {
  e.preventDefault();
  const text = els.input.value.trim();
  if (!text || abortController) return;
  els.input.value = "";
  sendMessage(text);
});

els.input.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    els.form.requestSubmit();
  }
});

els.stop.addEventListener("click", () => abortController?.abort());

els.newChat.addEventListener("click", () => {
  abortController?.abort();
  history = [];
  els.messages.innerHTML = "";
  lockConversationSettings(false);
});

// Thinking blocks we echo back are bound to the request they were produced in.
// If the tool list (or thinking config) changes mid-conversation, the API rejects
// the old thinking blocks with a 400. So these settings are fixed per conversation.
function lockConversationSettings(locked) {
  for (const control of [els.thinkingDisplay, els.enableTools, els.enableWebSearch]) {
    control.disabled = locked;
    control.title = locked ? "Fixed for this conversation; click New chat to change" : "";
  }
}

async function sendMessage(text) {
  lockConversationSettings(true);
  history.push({ role: "user", content: text });
  addUserBubble(text);
  const view = new AssistantView(els.messages);

  abortController = new AbortController();
  setBusy(true);
  try {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        messages: history,
        effort: els.effort.value,
        thinking_display: els.thinkingDisplay.value,
        enable_tools: els.enableTools.checked,
        enable_web_search: els.enableWebSearch.checked,
      }),
      signal: abortController.signal,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);

    for await (const { event, data } of readSSE(res.body)) {
      logEvent(event, data);
      if (event === "history") history.push(...data.messages);
      else view.handle(event, data);
    }
  } catch (err) {
    if (err.name === "AbortError") view.notice("warn", "Stopped by user. The partial reply is not kept in history.");
    else view.notice("error", String(err));
  } finally {
    view.finish();
    abortController = null;
    setBusy(false);
  }
}

// EventSource only supports GET, so we parse SSE ourselves from a fetch() body.
// Network chunks don't line up with events: one chunk may hold several events
// or half of one, so we buffer until we see the blank line that ends an event.
async function* readSSE(body) {
  const reader = body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) return;
    buffer += value.replace(/\r\n/g, "\n");
    let end;
    while ((end = buffer.indexOf("\n\n")) !== -1) {
      const raw = buffer.slice(0, end);
      buffer = buffer.slice(end + 2);
      let event = "message";
      const dataLines = [];
      for (const line of raw.split("\n")) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
      }
      if (dataLines.length) yield { event, data: JSON.parse(dataLines.join("\n")) };
    }
  }
}

function setBusy(busy) {
  els.send.hidden = busy;
  els.stop.hidden = !busy;
}

// ---------------------------------------------------------------------------
// Rendering one assistant reply (may span several API turns)
// ---------------------------------------------------------------------------

class AssistantView {
  constructor(container) {
    this.root = el("div", "msg assistant");
    container.appendChild(this.root);
    this.blocks = new Map(); // "turn:index" -> renderer
  }

  handle(event, data) {
    const key = `${data.turn}:${data.index}`;
    switch (event) {
      case "message_start":
        break;
      case "block_start": {
        const renderer = createRenderer(data.block);
        this.blocks.set(key, renderer);
        this.root.appendChild(renderer.el);
        break;
      }
      case "block_delta":
        this.blocks.get(key)?.delta(data.delta);
        break;
      case "block_stop":
        this.blocks.get(key)?.stop(data.block);
        break;
      case "message_delta":
        this.turnFooter(data);
        break;
      case "tool_result":
        this.root.appendChild(renderToolResult(data));
        break;
      case "error":
        this.notice("error", `${data.type}${data.status ? ` (${data.status})` : ""}: ${data.message}`);
        break;
    }
    scrollToBottom();
  }

  turnFooter({ turn, stop_reason, stop_details, usage }) {
    const parts = [`turn ${turn}`, `stop_reason=${stop_reason}`];
    if (usage?.input_tokens != null) parts.push(`in=${usage.input_tokens}`);
    if (usage?.output_tokens != null) parts.push(`out=${usage.output_tokens}`);
    this.root.appendChild(el("div", "turn-footer", parts.join(" · ")));
    if (stop_reason === "refusal") {
      const why = stop_details ? ` (${stop_details.category ?? "unspecified"}: ${stop_details.explanation ?? ""})` : "";
      this.notice("warn", `The model declined to continue${why}.`);
    } else if (stop_reason === "max_tokens") {
      this.notice("warn", "Hit max_tokens; the reply is truncated.");
    }
  }

  notice(kind, message) {
    this.root.appendChild(el("div", `notice ${kind}`, message));
    scrollToBottom();
  }

  finish() {
    for (const r of this.blocks.values()) r.el.classList.remove("streaming");
  }
}

// One renderer per content block type. Each has: el, delta(delta), stop(finalBlock).
function createRenderer(block) {
  switch (block.type) {
    case "text":
      return textRenderer();
    case "thinking":
      return thinkingRenderer();
    case "redacted_thinking":
      return staticRenderer("thinking", "Redacted thinking", "Encrypted reasoning; it is passed back to the API but can't be shown.");
    case "tool_use":
      return toolCallRenderer("tool_use", `Tool call · ${block.name}`);
    case "server_tool_use":
      return toolCallRenderer("server_tool_use", `Server tool · ${block.name}`);
    case "web_search_tool_result":
      return webSearchResultRenderer(block);
    case "fallback":
      return staticRenderer("fallback", "Fallback", `${block.from?.model} declined; ${block.to?.model} continued.`);
    default:
      return genericRenderer(block);
  }
}

function textRenderer() {
  const node = el("div", "block text streaming");
  const body = el("div", "md");
  const cites = el("ol", "citations");
  node.append(body, cites);
  let text = "";
  return {
    el: node,
    delta(d) {
      if (d.type === "text_delta") {
        // Re-render the whole accumulated text: a delta can split a markdown
        // token (e.g. "**bo" + "ld**"), so deltas can't be rendered on their own.
        text += d.text;
        renderMarkdown(body, text);
      } else if (d.type === "citations_delta") {
        const c = d.citation;
        const li = el("li");
        const a = el("a", "", c.title || c.url || c.document_title || "source");
        if (c.url) { a.href = c.url; a.target = "_blank"; a.rel = "noopener"; }
        li.append(a, ` — “${(c.cited_text || "").slice(0, 140)}”`);
        cites.appendChild(li);
      }
    },
    stop() { node.classList.remove("streaming"); },
  };
}

function thinkingRenderer() {
  const node = el("details", "block thinking streaming");
  node.open = true;
  const summary = el("summary", "", "Thinking…");
  const body = el("div", "body");
  node.append(summary, body);
  let signatureLength = 0;
  return {
    el: node,
    delta(d) {
      if (d.type === "thinking_delta") body.textContent += d.thinking;
      // The signature arrives in one delta just before block_stop. It is an
      // opaque token that lets the API verify the thinking when you send it back.
      else if (d.type === "signature_delta") signatureLength += d.signature.length;
    },
    stop() {
      node.classList.remove("streaming");
      summary.textContent = `Thinking · signature ${signatureLength} chars`;
      if (!body.textContent) body.textContent = "(no thinking text — display is \"omitted\", or the model skipped thinking)";
      node.open = false;
    },
  };
}

function toolCallRenderer(cls, title) {
  const node = el("div", `block ${cls} streaming`);
  const pre = el("pre");
  node.append(el("div", "card-title", title), pre);
  return {
    el: node,
    // input_json_delta carries *fragments* of JSON ("{\"expr", "ession\": \"2+"),
    // not valid JSON on its own — just show it growing.
    delta(d) { if (d.type === "input_json_delta") pre.textContent += d.partial_json; },
    stop(final) {
      node.classList.remove("streaming");
      if (final?.input) pre.textContent = JSON.stringify(final.input, null, 2);
    },
  };
}

function webSearchResultRenderer(block) {
  const node = el("div", "block web_search_tool_result");
  node.appendChild(el("div", "card-title", "Web search results"));
  // Success: content is a list of results. Error: content is a single object.
  if (Array.isArray(block.content)) {
    const ul = el("ul", "results");
    for (const r of block.content) {
      const a = el("a", "", r.title || r.url);
      a.href = r.url; a.target = "_blank"; a.rel = "noopener";
      const li = el("li"); li.appendChild(a); ul.appendChild(li);
    }
    node.appendChild(ul);
  } else {
    node.appendChild(el("pre", "", JSON.stringify(block.content, null, 2)));
  }
  return { el: node, delta() {}, stop() {} };
}

function staticRenderer(cls, title, text) {
  const node = el("div", `block ${cls}`);
  node.append(el("div", "card-title", title), el("div", "", text));
  return { el: node, delta() {}, stop() {} };
}

function genericRenderer(block) {
  const node = el("details", "block generic");
  const pre = el("pre", "", JSON.stringify(block, null, 2));
  node.append(el("summary", "", `Block · ${block.type}`), pre);
  return {
    el: node,
    delta(d) { pre.textContent += `\n${JSON.stringify(d)}`; },
    stop(final) { if (final) pre.textContent = JSON.stringify(final, null, 2); },
  };
}

function renderToolResult({ name, content, is_error }) {
  const node = el("div", `block tool_result${is_error ? " is-error" : ""}`);
  node.append(el("div", "card-title", `Tool result · ${name} (run by our server)`), el("pre", "", content));
  return node;
}

function addUserBubble(text) {
  const node = el("div", "msg user");
  node.appendChild(el("div", "bubble", text));
  els.messages.appendChild(node);
  scrollToBottom();
}

function scrollToBottom() {
  els.messages.scrollTop = els.messages.scrollHeight;
}

function renderMarkdown(node, text) {
  if (window.marked && window.DOMPurify) node.innerHTML = DOMPurify.sanitize(marked.parse(text));
  else node.textContent = text;
}

function el(tag, className = "", text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text != null) node.textContent = text;
  return node;
}

// ---------------------------------------------------------------------------
// Event inspector: every SSE event as it arrives
// ---------------------------------------------------------------------------

let eventCount = 0;
const MAX_LOG_ENTRIES = 3000;

function logEvent(event, data) {
  eventCount += 1;
  els.eventCount.textContent = eventCount;
  const li = el("li", event === "block_delta" ? "delta" : "");
  const json = JSON.stringify(data);
  li.append(el("span", "ev", event), " ", json.length > 300 ? `${json.slice(0, 300)}…` : json);
  const atBottom = els.log.scrollHeight - els.log.scrollTop - els.log.clientHeight < 40;
  els.log.appendChild(li);
  if (els.log.children.length > MAX_LOG_ENTRIES) els.log.firstChild.remove();
  if (atBottom) els.log.scrollTop = els.log.scrollHeight;
}

els.showDeltas.addEventListener("change", () => els.log.classList.toggle("hide-deltas", !els.showDeltas.checked));
els.clearLog.addEventListener("click", () => {
  els.log.innerHTML = "";
  eventCount = 0;
  els.eventCount.textContent = "0";
});
