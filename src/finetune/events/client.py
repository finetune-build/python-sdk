"""
In-memory event store for demonstrating resumability functionality.

This is a simple implementation intended for examples and testing,
not for production use where a persistent storage solution would be more appropriate.
"""

import logging
from collections import deque
from dataclasses import dataclass
from uuid import uuid4

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventMessage,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage

logger = logging.getLogger(__name__)


@dataclass
class EventEntry:
    """
    Represents an event entry in the event store.
    """

    event_id: EventId
    stream_id: StreamId
    message: JSONRPCMessage


class InMemoryEventStore(EventStore):
    """
    Simple in-memory implementation of the EventStore interface for resumability.
    This is primarily intended for examples and testing, not for production use
    where a persistent storage solution would be more appropriate.

    This implementation keeps only the last N events per stream for memory efficiency.
    """

    def __init__(self, max_events_per_stream: int = 100):
        """Initialize the event store.

        Args:
            max_events_per_stream: Maximum number of events to keep per stream
        """
        self.max_events_per_stream = max_events_per_stream
        # for maintaining last N events per stream
        self.streams: dict[StreamId, deque[EventEntry]] = {}
        # event_id -> EventEntry for quick lookup
        self.event_index: dict[EventId, EventEntry] = {}

    async def store_event(
        self, stream_id: StreamId, message: JSONRPCMessage
    ) -> EventId:
        """Stores an event with a generated event ID."""
        event_id = str(uuid4())
        event_entry = EventEntry(
            event_id=event_id, stream_id=stream_id, message=message
        )

        # Get or create deque for this stream
        if stream_id not in self.streams:
            self.streams[stream_id] = deque(maxlen=self.max_events_per_stream)

        # If deque is full, the oldest event will be automatically removed
        # We need to remove it from the event_index as well
        if len(self.streams[stream_id]) == self.max_events_per_stream:
            oldest_event = self.streams[stream_id][0]
            self.event_index.pop(oldest_event.event_id, None)

        # Add new event
        self.streams[stream_id].append(event_entry)
        self.event_index[event_id] = event_entry

        return event_id

    async def replay_events_after(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
        """Replays events that occurred after the specified event ID."""
        if last_event_id not in self.event_index:
            logger.warning(f"Event ID {last_event_id} not found in store")
            return None

        # Get the stream and find events after the last one
        last_event = self.event_index[last_event_id]
        stream_id = last_event.stream_id
        stream_events = self.streams.get(last_event.stream_id, deque())

        # Events in deque are already in chronological order
        found_last = False
        for event in stream_events:
            if found_last:
                await send_callback(EventMessage(event.message, event.event_id))
            elif event.event_id == last_event_id:
                found_last = True

        return stream_id








from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.lowlevel import Server

# MCP Proxy
event_store = InMemoryEventStore()

app = Server("mcp-streamable-http-proxy")

import anyio

from mcp.types import ContentBlock, TextContent, Tool
from pydantic import AnyUrl

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[ContentBlock]:
    ctx = app.request_context
    interval = 1.0
    count = 5
    caller = "unknown"

    # Send the specified number of notifications with the given interval
    for i in range(count):
        # Include more detailed message for resumability demonstration
        notification_msg = (
            f"[{i + 1}/{count}] Event from '{caller}' - "
            f"Use Last-Event-ID to resume if disconnected"
        )
        await ctx.session.send_log_message(
            level="info",
            data=notification_msg,
            logger="notification_stream",
            # Associates this notification with the original request
            # Ensures notifications are sent to the correct response stream
            # Without this, notifications will either go to:
            # - a standalone SSE stream (if GET request is supported)
            # - nowhere (if GET request isn't supported)
            related_request_id=ctx.request_id,
        )
        print(f"Sent notification {i + 1}/{count} for caller: {caller}")
        if i < count - 1:  # Don't wait after the last notification
            await anyio.sleep(interval)

    # This will send a resource notificaiton though standalone SSE
    # established by GET request
    await ctx.session.send_resource_updated(uri=AnyUrl("http:///test_resource"))
    return [
        TextContent(
            type="text",
            text=(
                f"Sent {count} notifications with {interval}s interval"
                f" for caller: {caller}"
            ),
        )
    ]

@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="start-notification-stream",
            description=(
                "Sends a stream of notifications with configurable count"
                " and interval"
            ),
            inputSchema={
                "type": "object",
                # "required": ["interval", "count", "caller"],
                # "properties": {
                #     "interval": {
                #         "type": "number",
                #         "description": "Interval between notifications in seconds",
                #     },
                #     "count": {
                #         "type": "number",
                #         "description": "Number of notifications to send",
                #     },
                #     "caller": {
                #         "type": "string",
                #         "description": (
                #             "Identifier of the caller to include in notifications"
                #         ),
                #     },
                # },
            },
        )
    ]
# Create the session manager with our app and event store
session_manager = StreamableHTTPSessionManager(
    app=app,
    event_store=event_store,  # Enable resumability
    json_response=False,
)





import asyncio
from typing import MutableMapping, Any, Callable, Awaitable

# ASGI types
Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

def make_receive(reader: asyncio.StreamReader) -> Receive:
    """
    Reads from the real TCP stream and returns an ASGI http.request message.
    Only reads once because this example assumes no body streaming.
    """
    received = False

    async def receive():
        nonlocal received
        if not received:
            received = True
            body = await reader.read(65536)
            return {
                "type": "http.request",
                "body": body,
                "more_body": False,
            }
        else:
            # After body consumed, simulate disconnect or wait for more data
            await asyncio.sleep(3600)
            return {"type": "http.disconnect"}

    return receive

def make_send(writer: asyncio.StreamWriter) -> Send:
    async def send(message: Message):
        if message["type"] == "http.response.start":
            status = message["status"]
            headers = message.get("headers", [])
            print(f"[Client] Response started: {status}")
            writer.write(f"HTTP/1.1 {status} OK\r\n".encode())
            for name, value in headers:
                writer.write(name + b": " + value + b"\r\n")
            writer.write(b"\r\n")
            await writer.drain()

        elif message["type"] == "http.response.body":
            body = message["body"]
            print(f"[Client] Received body chunk: {body.decode(errors='ignore')}")
            writer.write(body)
            await writer.drain()
            if not message.get("more_body", False):
                print("[Client] Final chunk received, closing...")
                writer.close()
                await writer.wait_closed()

    return send


def build_scope() -> Scope:
    """
    Build an ASGI HTTP scope that matches your request.
    """
    return {
        "type": "http",
        "http_version": "1.1",
        "asgi.version": "3.0",
        "asgi.spec_version": "2.1",
        "method": "GET",
        "scheme": "https",
        "path": "/v1/worker/3qssEZ8hV7tSPAMumoYhLu/streamable_http/",
        "query_string": b"",
        "headers": [
            (b"host", b"api.finetune.engineering"),
            (b"accept", b"text/event-stream"),
            (b"user-agent", b"stream-client/1.0"),
        ],
        "client": ("127.0.0.1", 5555),
        "server": ("api.finetune.engineering", 8000),
    }

async def main():
    import ssl

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    reader, writer = await asyncio.open_connection(
        "api.finetune.engineering",
        8000,
        ssl=ssl_context,
        server_hostname="api.finetune.engineering",
    )

    # Send the initial raw HTTP request bytes
    request = (
        "GET /v1/worker/3qssEZ8hV7tSPAMumoYhLu/streamable_http/ HTTP/1.1\r\n"
        "Host: developmentpackage.finetune.engineering\r\n"
        "Accept: text/event-stream\r\n"
        "Connection: keep-alive\r\n"
        "\r\n"
    )
    writer.write(request.encode())
    await writer.drain()

    # Build the ASGI scope
    scope = build_scope()

    # Wrap reader and writer in ASGI receive/send
    receive = make_receive(reader)
    send = make_send(writer)

    # Now run the session manager ASGI handler over this real network stream
    async with session_manager.run():
        await session_manager.handle_request(scope, receive, send)

if __name__ == "__main__":
    asyncio.run(main())

