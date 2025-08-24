import aiohttp
import asyncio
import ssl
import json

from finetune.api.worker import worker_pong
from finetune.conf import settings
from finetune.ws.conversation import start_conversation_thread, shutdown_conversation_thread
from finetune.ws.worker import worker_start_websocket_thread

from finetune.utils.redis_client import RedisClient
from finetune.api.worker import worker_mcp_response

from finetune.events.streamable_http import async_stream_client

class EventListener:
    def __init__(self, redis_client: RedisClient = None):
        self.redis_client = redis_client 
        self.session = None
        self._stop_event = asyncio.Event()
        self.url = f"https://{settings.DJANGO_HOST}/v1/worker/{settings.WORKER_ID}/sse/"
        self.headers = {
            "Authorization": f"Access {settings.ACCESS_TOKEN}",
            "X-Worker-ID": settings.WORKER_ID,
            # "X-Session-ID": str(settings.SESSION_UUID),
            "X-Client-Role": "machine",
        }
    
    async def start(self):
        """
        Opens stream with API server for SSE.
        """
        # Create session with connection timeout but no read timeout for SSE
        timeout = aiohttp.ClientTimeout(sock_read=None, connect=10, total=None)
        self.session = aiohttp.ClientSession(timeout=timeout)
        
        try:
            print(f"[EventListener] Connecting to {self.url}")
            response = await self.session.get(self.url, ssl=False, headers=self.headers)
            
            if response.status != 200:
                error_details = await response.text()
                print(f"Error details: {error_details}")
                response.raise_for_status()
            
            print(f"Connected as {settings.WORKER_ID}, status: {response.status}")
            
            # Read stream until stopped or connection closes
            async for line in response.content:
                # Check if we should stop
                if self._stop_event.is_set():
                    print("Stop requested, closing connection")
                    break
                    
                decoded = line.decode("utf-8").strip()
                if decoded.startswith("data:"):
                    message = decoded[5:].strip()
                    try:
                        data = json.loads(message)
                        await self.on_event(data)
                    except json.JSONDecodeError:
                        print(f"Received non-JSON message: {message}")
                elif decoded.startswith(":"):
                    print("Heartbeat")
                    
        except Exception as e:
            print(f"[EventListener] Connection error: {e}")
            raise
        finally:
            # Always clean up session
            if self.session:
                await self.session.close()

    async def on_event(self, data):
        """
        Handle JSON-RPC 2.0 formatted requests.
        """
        print(data)
        method = data.get("method")
        params = data.get("params", {})
        request_id = data.get("id")
    
        if method == "worker_ping" or method == "worker_ping_all_active":
            print("Worker Ping Received. Sending pong...")
            await worker_pong()
            return {
                "jsonrpc": "2.0",
                "result": "pong",
                "id": request_id,
            }

        elif method == "mcp_connect":
            print("called mcp connect...")
            await async_stream_client(params["initialization_id"])
    
        elif method == "worker_mcp_request":
            print("Received worker MCP request event")
            if self.redis_client is None:
                return {
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32601,
                        "message": f"Event listener pubsub not initialized"
                    },
                    "id": request_id,
                }
            else:
                print(f"params: {params}")
                self.redis_client.publish('mcp_requests', data)
                return {
                    "jsonrpc": "2.0",
                    "result": "MCP request processed",
                    "id": request_id,
                }
    
        elif method == "worker_task_created":
            print(f"Received Worker Task")
            return {
                "jsonrpc": "2.0",
                "result": f"Worker {settings.WORKER_ID} received task",
                "id": request_id,
            }
    
        elif method == "worker_start_websocket_thread":
            print(f"Starting Worker Websocket Thread: {settings.WORKER_ID}")
            worker_start_websocket_thread(settings.WORKER_ID)
            return {
                "jsonrpc": "2.0",
                "result": f"Worker {settings.WORKER_ID} websocket opened",
                "id": request_id,
            }
    
        elif method == "conversation_open_websocket":
            content = params.get("content")
            conversation_id = params.get("conversation_id")
            print(f"Starting Conversation Websocket Thread: {conversation_id}")
            start_conversation_thread(conversation_id, content)
            return {
                "jsonrpc": "2.0",
                "result": f"Conversation {conversation_id} websocket opened",
                "id": request_id,
            }
    
        elif method == "conversation_close_websocket":
            conversation_id = params.get("conversation_id")
            print("Closing WebSocket connection for conversation in a thread...")
            shutdown_conversation_thread(conversation_id)
            return {
                "jsonrpc": "2.0",
                "result": f"Conversation {conversation_id} websocket closed",
                "id": request_id,
            }
    
        else:
            print(f"Received unknown method: {method}")
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32601,
                    "message": f"Method '{method}' not found"
                },
                "id": request_id,
            }
    
    async def send_worker_mcp_response(self, response, correlation_id):
        print(f"sending response back: {response}")
        await worker_mcp_response(response, correlation_id)

    async def stop(self):
        """Stop the event listener gracefully."""
        self._stop_event.set()
    
    def is_running(self) -> bool:
        """Check if the listener is still running."""
        return not self._stop_event.is_set()
