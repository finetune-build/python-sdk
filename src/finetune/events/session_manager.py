from .event_store import InMemoryEventStore

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
