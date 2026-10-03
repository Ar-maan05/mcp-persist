"""A local, read-only web view of what a store actually contains.

``mcp-persist stats`` answers "how many events", and ``dump`` answers "what
exactly is in this one stream", but neither is much good when the question is
the vague one you actually have at 2am: *is anything arriving, and does it look
right?* This serves a single self-contained page that lists the streams, lets
you click into one and read its events, shows the durable sessions, and
refreshes itself.

    mcp-persist dashboard

It binds to 127.0.0.1 by default and never writes: every route reads through the
same store the server uses, so it shows the decrypted, decompressed truth rather
than raw rows. It is a debugging tool for a store you already have access to,
not a monitoring service; there is no authentication, which is exactly why it
refuses to bind a non-loopback address unless you say ``--unsafe-bind``.

Binding to loopback is not enough on its own: a web page the operator happens to
have open can re-point its own hostname at 127.0.0.1 (DNS rebinding) and read
the API as same-origin. So every request must name the dashboard by a host it
actually answers to, and anything else is refused before it reaches a route.

The page has no external assets. The CSS and JS are inline, so it works on a
locked-down network, in an air-gapped environment, and behind a corporate proxy,
and there is no CDN to trust.
"""

from __future__ import annotations

import json
from collections import deque
from typing import TYPE_CHECKING, Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.routing import Route

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from starlette.requests import Request
    from starlette.types import ASGIApp, Receive, Scope, Send

    from mcp_persist._admin import StoreConfig

MAX_PAYLOAD_PREVIEW = 4000

LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1")


def _host_name(header: str) -> str:
    """The host part of a ``Host`` header, lower-cased, without port or brackets."""
    header = header.strip().lower()
    if header.startswith("["):  # IPv6 literal, e.g. "[::1]:8765"
        return header[1 : header.find("]")] if "]" in header else header
    return header.rsplit(":", 1)[0] if header.count(":") == 1 else header


class _HostGuard:
    """Refuse any request whose ``Host`` is not one the dashboard answers to.

    Starlette ships a ``TrustedHostMiddleware``, but it splits the header on the
    first colon, which cannot match an IPv6 literal such as ``[::1]:8765``.
    """

    def __init__(self, app: ASGIApp, *, hosts: Iterable[str]) -> None:
        self._app = app
        self._hosts = frozenset(h.strip("[]").lower() for h in hosts)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] in ("http", "websocket") and "*" not in self._hosts:
            host = next((v.decode("latin-1") for k, v in scope["headers"] if k == b"host"), "")
            if _host_name(host) not in self._hosts:
                response = PlainTextResponse("Invalid host header", status_code=400)
                await response(scope, receive, send)
                return
        await self._app(scope, receive, send)


def _describe_payload(raw: Any) -> dict[str, Any]:
    """Summarize one JSON-RPC message for the table, keeping the full text too.

    The list is far more readable with a method or a result/error label than with
    a wall of JSON, but the JSON is what you came for once something looks wrong,
    so both are sent and the page decides.
    """
    if raw is None:
        return {"kind": "priming", "label": "(stream primed, no message)", "json": None}

    text = json.dumps(raw, indent=2, sort_keys=True)
    truncated = len(text) > MAX_PAYLOAD_PREVIEW
    if truncated:
        text = text[:MAX_PAYLOAD_PREVIEW] + "\n… truncated"

    kind, label = "message", "message"
    if isinstance(raw, dict):
        if raw.get("method"):
            kind = "notification" if raw.get("id") is None else "request"
            label = str(raw["method"])
        elif "error" in raw:
            error = raw.get("error")
            code = error.get("code") if isinstance(error, dict) else None
            kind, label = "error", f"error {code}" if code is not None else "error"
        elif "result" in raw:
            kind, label = "result", "result"
    return {"kind": kind, "label": label, "json": text, "truncated": truncated}


async def _snapshot(cfg: StoreConfig, store: Any) -> dict[str, Any]:
    """Everything the overview needs, in one round trip per section."""
    from mcp_persist._admin import gather_stats, redact_url

    report = await gather_stats(cfg, store)

    health: dict[str, Any] = {"healthy": None}
    probe = getattr(store, "health", None)
    if probe is not None:
        try:
            health = (await probe()).as_dict()
        except Exception as exc:  # pragma: no cover - health() is documented not to raise
            health = {"healthy": False, "detail": {"error": str(exc)}}

    return {
        "backend": report.backend,
        "url": redact_url(cfg.url),
        "tenant_id": cfg.tenant_id,
        "ttl": cfg.ttl,
        "total_events": report.total_events,
        "total_streams": report.total_streams,
        "last_event_id": report.last_event_id,
        "latency_ms": round(report.latency_ms, 2),
        "health": health,
        "streams": [
            {
                "stream_id": s.stream_id,
                "events": s.events,
                "min_event_id": s.min_event_id,
                "max_event_id": s.max_event_id,
            }
            for s in report.streams
        ],
    }


async def _sessions(store: Any) -> dict[str, Any]:
    """Durable sessions, or an explanation of why there are none to show."""
    from mcp_persist.sessions import session_registry_for

    try:
        registry = session_registry_for(store)
        await registry.initialize()
    except TypeError as exc:
        return {"available": False, "reason": str(exc), "sessions": []}

    records = await registry.list_sessions(include_terminated=True, limit=200)
    return {
        "available": True,
        "reason": None,
        "sessions": [r.as_dict() for r in records],
    }


def create_dashboard(
    cfg: StoreConfig, *, redact_payloads: bool = False, allowed_hosts: Iterable[str] | None = None
) -> Starlette:
    """Build the dashboard ASGI app for the store described by ``cfg``.

    The store is opened once for the app's lifetime rather than per request, so
    the page does not reconnect on every poll.

    Args:
        cfg: The resolved store settings (the CLI builds this from flags/env).
        redact_payloads: Omit message bodies, leaving counts, ids, timestamps and
            the method names. Use it when the screen is shared, or when the
            events carry data you would rather not render.
        allowed_hosts: Host names a request may address the dashboard by.
            Defaults to the loopback names; ``"*"`` accepts any, which is only
            appropriate when the operator has deliberately exposed it.
    """
    import contextlib

    from mcp_persist._admin import _build_store, _quiet_package_log

    state: dict[str, Any] = {"store": None}

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        with _quiet_package_log():
            async with _build_store(cfg) as store:
                state["store"] = store
                yield

    async def index(_: Request) -> HTMLResponse:
        return HTMLResponse(_PAGE)

    async def api_overview(_: Request) -> JSONResponse:
        try:
            payload = await _snapshot(cfg, state["store"])
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=503)
        payload["redact_payloads"] = redact_payloads
        return JSONResponse(payload)

    async def api_sessions(_: Request) -> JSONResponse:
        try:
            return JSONResponse(await _sessions(state["store"]))
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=503)

    async def api_events(request: Request) -> JSONResponse:
        stream_id = request.path_params["stream_id"]
        try:
            limit = max(1, min(int(request.query_params.get("limit", 200)), 1000))
        except ValueError:
            limit = 200

        # Only the newest `limit` events are kept, so a long stream costs a read
        # rather than a full in-memory export on every poll.
        newest: deque[tuple[str, Any]] = deque(maxlen=limit)
        total = 0
        try:
            async for event_id, message in state["store"]._iter_stream_events(stream_id):
                newest.append((str(event_id), message))
                total += 1
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=503)

        # Newest first: when a stream is long, the recent end is the interesting
        # one, and it saves the page scrolling to the bottom on every refresh.
        rows = [
            {
                "event_id": event_id,
                "payload": {"kind": "redacted", "label": "(redacted)", "json": None}
                if redact_payloads
                else _describe_payload(
                    None if message is None else message.model_dump(mode="json", by_alias=True, exclude_none=True)
                ),
            }
            for event_id, message in reversed(newest)
        ]
        return JSONResponse({"stream_id": stream_id, "events": rows, "truncated": total > limit})

    return Starlette(
        lifespan=lifespan,
        middleware=[Middleware(_HostGuard, hosts=LOOPBACK_HOSTS if allowed_hosts is None else allowed_hosts)],
        routes=[
            Route("/", index),
            Route("/api/overview", api_overview),
            Route("/api/sessions", api_sessions),
            Route("/api/streams/{stream_id:path}/events", api_events),
        ],
    )


_PAGE = """
<!-- Deliberately self-contained: no CDN, no external font, no build step. -->
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>mcp-persist</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #ffffff; --panel: #f6f7f9; --line: #e2e5ea; --text: #14171c;
    --muted: #667085; --accent: #3b5bdb; --ok: #1f9254; --bad: #c92a2a;
    --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0f1115; --panel: #171a21; --line: #262b35; --text: #e6e8eb;
      --muted: #98a2b3; --accent: #8ea3ff; --ok: #4ade80; --bad: #ff7b72;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  header {
    display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap;
    padding: 14px 20px; border-bottom: 1px solid var(--line);
    position: sticky; top: 0; background: var(--bg); z-index: 5;
  }
  h1 { font-size: 15px; margin: 0; font-weight: 650; letter-spacing: -0.01em; }
  .grow { flex: 1; }
  .muted { color: var(--muted); }
  .mono { font-family: var(--mono); }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
  .dot.ok { background: var(--ok); } .dot.bad { background: var(--bad); }
  main { padding: 20px; display: grid; gap: 20px; max-width: 1200px; }
  .tiles { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); }
  .tile { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; }
  .tile .k { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .06em; }
  .tile .v { font-size: 22px; font-weight: 600; margin-top: 4px; font-variant-numeric: tabular-nums; }
  section > h2 { font-size: 12px; text-transform: uppercase; letter-spacing: .07em;
                 color: var(--muted); margin: 0 0 10px; font-weight: 600; }
  .scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 10px; }
  table { border-collapse: collapse; width: 100%; min-width: 520px; }
  th, td { text-align: left; padding: 9px 12px; border-bottom: 1px solid var(--line); }
  th { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); font-weight: 600; }
  tr:last-child td { border-bottom: 0; }
  tbody tr.clickable { cursor: pointer; }
  tbody tr.clickable:hover { background: var(--panel); }
  tr.sel { background: var(--panel); }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .tag { font-size: 11px; padding: 1px 7px; border-radius: 999px; border: 1px solid var(--line); }
  .tag.request { color: var(--accent); } .tag.notification { color: var(--muted); }
  .tag.result { color: var(--ok); } .tag.error { color: var(--bad); }
  pre { margin: 6px 0 0; padding: 10px; background: var(--panel); border-radius: 8px;
        overflow-x: auto; font-family: var(--mono); font-size: 12px; }
  .empty { padding: 18px; color: var(--muted); }
  button { font: inherit; color: inherit; background: var(--panel); cursor: pointer;
           border: 1px solid var(--line); border-radius: 7px; padding: 4px 10px; }
  .err { color: var(--bad); }
</style>

<header>
  <h1>mcp-persist</h1>
  <span class="muted mono" id="target"></span>
  <span class="grow"></span>
  <span id="health" class="muted"></span>
  <button id="pause">Pause</button>
</header>

<main>
  <div class="tiles" id="tiles"></div>

  <section>
    <h2>Streams</h2>
    <div class="scroll">
      <table>
        <thead><tr><th>Stream</th><th class="num">Events</th>
          <th class="num">First</th><th class="num">Last</th></tr></thead>
        <tbody id="streams"></tbody>
      </table>
    </div>
  </section>

  <section id="eventsSection" hidden>
    <h2>Events <span class="mono muted" id="eventsFor"></span></h2>
    <div class="scroll">
      <table>
        <thead><tr><th class="num">ID</th><th>Type</th><th>Message</th></tr></thead>
        <tbody id="events"></tbody>
      </table>
    </div>
  </section>

  <section>
    <h2>Sessions</h2>
    <div class="scroll">
      <table>
        <thead><tr><th>Session</th><th>Client</th><th>Last seen</th><th>State</th></tr></thead>
        <tbody id="sessions"></tbody>
      </table>
    </div>
  </section>
</main>

<script>
const $ = (id) => document.getElementById(id);
let selected = null, paused = false, openRow = null, loadedMark = null;

const esc = (s) => String(s).replace(/[&<>"']/g, c => (
  {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const when = (t) => t ? new Date(t * 1000).toLocaleString() : '';

$('pause').onclick = () => {
  paused = !paused;
  $('pause').textContent = paused ? 'Resume' : 'Pause';
  if (!paused) refresh();
};

async function getJSON(url) {
  const r = await fetch(url);
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

function tile(k, v) {
  return `<div class="tile"><div class="k">${esc(k)}</div><div class="v">${esc(v)}</div></div>`;
}

async function refresh() {
  if (paused) return;
  try {
    const o = await getJSON('/api/overview');
    $('target').textContent = `${o.backend} · ${o.url}` + (o.tenant_id ? ` · tenant ${o.tenant_id}` : '');
    const ok = o.health.healthy !== false;
    $('health').innerHTML =
      `<span class="dot ${ok ? 'ok' : 'bad'}"></span> ${ok ? 'healthy' : 'unreachable'} · ${o.latency_ms} ms`;

    $('tiles').innerHTML = [
      tile('Events', o.total_events.toLocaleString()),
      tile('Streams', o.total_streams.toLocaleString()),
      tile('Last event id', o.last_event_id ?? 'none'),
      tile('TTL', o.ttl ? o.ttl + ' s' : 'none'),
    ].join('');

    $('streams').innerHTML = o.streams.length ? o.streams.map(s => `
      <tr class="clickable ${s.stream_id === selected ? 'sel' : ''}" data-id="${esc(s.stream_id)}">
        <td class="mono">${esc(s.stream_id)}</td>
        <td class="num">${s.events.toLocaleString()}</td>
        <td class="num mono">${s.min_event_id ?? ''}</td>
        <td class="num mono">${s.max_event_id ?? ''}</td>
      </tr>`).join('')
      : '<tr><td colspan="4" class="empty">No events stored yet.</td></tr>';

    for (const row of $('streams').querySelectorAll('tr[data-id]')) {
      row.onclick = () => { selected = row.dataset.id; openRow = null; loadedMark = null; refresh(); };
    }
    if (selected) {
      // Re-read the events only when the stream has moved on since the last read.
      const current = o.streams.find(s => s.stream_id === selected);
      const mark = `${selected}@${current ? current.max_event_id : ''}`;
      if (mark !== loadedMark) { loadedMark = mark; await loadEvents(selected); }
    }
  } catch (e) {
    $('health').innerHTML = `<span class="dot bad"></span> <span class="err">${esc(e.message)}</span>`;
  }

  try {
    const s = await getJSON('/api/sessions');
    $('sessions').innerHTML = !s.available
      ? `<tr><td colspan="4" class="empty">${esc(s.reason)}</td></tr>`
      : (s.sessions.length ? s.sessions.map(r => `
          <tr>
            <td class="mono">${esc(r.session_id)}</td>
            <td>${esc(r.client ?? '')}</td>
            <td>${esc(when(r.last_seen_at))}</td>
            <td>${r.terminated ? '<span class="tag error">terminated</span>'
                               : '<span class="tag result">live</span>'}</td>
          </tr>`).join('')
        : '<tr><td colspan="4" class="empty">No sessions recorded. Is durable_sessions enabled?</td></tr>');
  } catch (e) {
    $('sessions').innerHTML = `<tr><td colspan="4" class="empty err">${esc(e.message)}</td></tr>`;
  }
}

async function loadEvents(streamId) {
  $('eventsSection').hidden = false;
  $('eventsFor').textContent = streamId;
  try {
    const d = await getJSON('/api/streams/' + encodeURIComponent(streamId) + '/events');
    $('events').innerHTML = d.events.length ? d.events.map((e, i) => `
      <tr class="${e.payload.json ? 'clickable' : ''}" data-i="${i}">
        <td class="num mono">${e.event_id ?? ''}</td>
        <td><span class="tag ${esc(e.payload.kind)}">${esc(e.payload.kind)}</span></td>
        <td class="mono">${esc(e.payload.label)}
          ${e.payload.json && i === openRow ? `<pre>${esc(e.payload.json)}</pre>` : ''}</td>
      </tr>`).join('')
      : '<tr><td colspan="3" class="empty">This stream has no events.</td></tr>';

    for (const row of $('events').querySelectorAll('tr[data-i]')) {
      row.onclick = () => {
        const i = Number(row.dataset.i);
        openRow = openRow === i ? null : i;   // click again to collapse
        loadEvents(streamId);
      };
    }
  } catch (e) {
    $('events').innerHTML = `<tr><td colspan="3" class="empty err">${esc(e.message)}</td></tr>`;
  }
}

refresh();
setInterval(refresh, 2000);
</script>
"""
