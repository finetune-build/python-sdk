# http_client.py
import asyncio
import os
import logging
import mcp.types as types
from typing import Callable, Any, Optional

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from finetune.utils.redis_client import RedisClient 

class HTTPClient:
    def __init__(self, redis_client: RedisClient):
        self.redis_client = redis_client
        self.session = None
        self._stop_event = asyncio.Event()
        self._client_task = None
        self.capabilities = None
        self.logger = logging.getLogger(self.__class__.__name__)

    async def start(self):
        """Start and run the MCP client."""
        self.logger.info("Starting MCP client...")
        try:
            self.logger.debug(f"Current working directory: {os.getcwd()}")
            self.logger.info("Attempting to connect to MCP server...")

            async with streamablehttp_client("http://127.0.0.1:8000/mcp/") as (read_stream, write_stream, _):
                await asyncio.sleep(0.5)  # Give transport time to initialize
                self.logger.info("Transport connection established")
                self.logger.debug(f"Read stream: {read_stream}")
                self.logger.debug(f"Write stream: {write_stream}")
                
                # Test if we can write to the stream
                try:
                    # Try to check if the write stream is functional
                    self.logger.debug("Testing write stream...")
                    # Don't actually send anything yet, just check the stream
                    self.logger.debug(f"Write stream type: {type(write_stream)}")
                    self.logger.debug(f"Write stream attributes: {dir(write_stream)}")
                except Exception as e:
                    self.logger.error(f"Error checking write stream: {e}")

                self.session = ClientSession(read_stream, write_stream)
                await self.session.__aenter__()  # Manually enter async context

                try:
                    self.logger.info("Initializing session...")
                    result = await self.session.initialize()
                    self.logger.info("Session initialized successfully")
                    self.logger.debug(f"Initialize result: {result}")
                    if result:
                        self.capabilities = result.capabilities

                    self.logger.info("Listing available tools...")
                    tools = await self.session.list_tools()
                    for tool in tools.tools:
                        self.logger.info(f"  Tool: {tool.name} - {tool.description}")

                    self.logger.info("Listing available resources...")
                    resources = await self.session.list_resources()
                    for resource in resources.resources:
                        self.logger.info(f"  Resource: {resource.uri} - {resource.name}")

                    self.logger.info("Testing echo tool...")
                    echo_result = await self.session.call_tool(
                        "echo",
                        arguments={"message": "Hello from MCP client!"}
                    )
                    self.logger.info(f"Echo result: {echo_result.content}")

                    self.logger.info("Testing add tool...")
                    add_result = await self.session.call_tool(
                        "add",
                        arguments={"a": 5, "b": 3}
                    )
                    self.logger.info(f"Add result: {add_result.content}")

                    self.logger.info("Reading server info resource...")
                    content, mime_type = await self.session.read_resource("info://server")
                    self.logger.info(f"Server info: {content}")

                    await self._stop_event.wait()
                except Exception as e:
                    self.logger.error(f"Exception during session operation: {type(e).__name__}: {e}", exc_info=True)

                finally:
                    self.logger.info("Cleaning up session...")
                    await self.session.__aexit__(None, None, None)

        except Exception as e:
            self.logger.error(f"Exception in MCP client: {type(e).__name__}: {e}", exc_info=True)
        finally:
            self.logger.info("MCP client stopped")
            self.session = None

    async def stop(self):
        """Stop the MCP client gracefully."""
        self.logger.info("Stopping MCP client...")
        self._stop_event.set()

    async def handle_request(self, request: dict[str, Any]) -> dict[str, Any]:
        if not self.session:
            self.logger.error("Client not connected - cannot handle request")
            raise RuntimeError("Client not connected")

        try:
            self.logger.debug(f"Processing request: {request}")
            result = None
            correlation_id = request["id"] 
            request_data = request["params"]
            params = request_data.get("params", {})
            method = request_data.get("method")

            if method == "ping":
                result = await self.session.send_ping()
                result = result.model_dump(exclude_none=True)
            elif method == "server/capabilities":
                # Custom method to retrieve server capabilities from `result`
                # obtained during `result = await self.session.initialize()`.
                result = self.capabilities.model_dump(exclude_none=True)
            elif method == "completion/complete":
                # context = params.get("context", None)
                result = await self.session.complete(
                    ref=params["ref"],
                    argument=params["argument"],
                    # context=context
                )
                result = result.model_dump(exclude_none=True)
            elif method == "prompts/list":
                result = await self.session.list_prompts()
                result = result.model_dump(exclude_none=True)
            elif method == "prompts/get":
                result = await self.session.get_prompt(name=params["name"], arguments=params["arguments"] )
                result = result.model_dump(exclude_none=True)
            elif method == "resources/subscribe":
                result = await self.session.subscribe_resource()
                result = result.model_dump(exclude_none=True)
            elif method == "resources/unsubscribe":
                result = await self.session.unsubscribe_resource()
                result = result.model_dump(exclude_none=True)
            elif method == "resources/read":
                result = await self.session.read_resource(uri=params["uri"])
                result = result.model_dump(exclude_none=True)
            elif method == "resources/list":
                result = await self.session.list_resources()
                result = result.model_dump(exclude_none=True)
            elif method == "resources/templates/list":
                result = await self.session.list_resource_templates()
                result = result.model_dump(exclude_none=True)
            elif method == "tools/list":
                result = await self.session.list_tools()
                result = result.model_dump(exclude_none=True)
            elif method == "tools/call":
                result = await self.session.call_tool(name=params["name"], arguments=params["arguments"])
                result = result.model_dump(exclude_none=True)
            else:
                raise ValueError(f"Unknown method: {method}")

            response = {"jsonrpc": "2.0", "result": result, "id": request.get("id")}
            self.logger.debug("Received MCP response successfully")
            wrapped_response = {
                "jsonrpc": "2.0",
                "result": response,
                "id": correlation_id 
            }
            
            if self.redis_client is None:
                self.logger.error("Redis client not initialized")
                return {
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32601,
                        "message": "Event listener pubsub not initialized"
                    },
                    "id": None,
                }
            else:
                self.redis_client.publish('mcp_responses', wrapped_response)
            return wrapped_response

        except Exception as e:
            self.logger.error(f"Request error: {e}", exc_info=True)
            response = {
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": str(e)},
                "id": correlation_id 
            }
            return response