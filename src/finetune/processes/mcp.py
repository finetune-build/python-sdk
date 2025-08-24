import asyncio
import httpx
import importlib.util
import json
import sys
import ssl
import traceback
import uvicorn

from mcp.server.fastmcp import FastMCP
from pathlib import Path
from starlette.applications import Starlette
from typing import Any, cast
from typing_extensions import override
from uuid import uuid4, UUID

from finetune.api.worker import worker_relay
from finetune.conf import settings
from finetune.processes.base import BaseProcess

class MCPProcess(BaseProcess):
    """Simplified process manager for MCP HTTP servers."""
    
    def __init__(
        self,
        server_file: str = "examples/mcp_http_server.py",
        host: str = "127.0.0.1",
        port: int = 8001,
        reload: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.server_file: str = server_file
        self.host: str = host
        self.port: int = port
        self.reload: bool = reload
        self.relay_id: UUID | None = uuid4()
        self.app: Starlette | None = None
        
    def load_server_app(self, module_name:str = "mcp") -> Starlette:
        """Load the ASGI app from the server file."""
        try:
            server_path = Path(self.server_file).resolve()
            
            if not server_path.exists():
                examples_path = Path("examples") / Path(self.server_file).name
                if examples_path.exists():
                    server_path = examples_path.resolve()
                else:
                    raise FileNotFoundError(f"Server file not found: {self.server_file}")
            
            self.logger.info(f"Loading server from: {server_path}")
            
            # Load module from file
            spec = importlib.util.spec_from_file_location("user_mcp_server", server_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not load spec for {server_path}")
                
            module = importlib.util.module_from_spec(spec)
            sys.modules["user_mcp_server"] = module
            spec.loader.exec_module(module)
            
            # Try different ways to get the app
            app: Starlette | None = None

            if hasattr(module, module_name):
                mcp = cast(FastMCP, getattr(module, module_name))
                self.logger.info("Found FastMCP server, creating ASGI app")
                app = mcp.streamable_http_app()
            
            if app is None:
                # List what we found for debugging
                attributes = [attr for attr in dir(module) if not attr.startswith('_')]
                self.logger.error(f"Module attributes: {attributes}")
                
                raise AttributeError(f"No ASGI app found in {self.server_file}. Expected 'mcp' with streamable_http_app() method")
            return app
            
        except Exception as e:
            self.logger.error(f"Failed to load server app: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    async def run_server(self):
        """Run the local Uvicorn server and maintain proxy connection together."""
        try:
            self.logger.info("Loading ASGI app...")
            self.app = self.load_server_app()
            self.logger.info(f"ASGI app loaded successfully: {type(self.app)}")
    
            # Start local Uvicorn server in background
            self.logger.info(f"Starting local MCP server on {self.host}:{self.port}")
            config = uvicorn.Config(
                self.app,
                host=self.host,
                port=self.port,
                reload=self.reload,
                log_level="info",
                access_log=True,
            )
            server = uvicorn.Server(config)
            local_server_task = asyncio.create_task(server.serve())
    
            # Start proxy connection in background
            self.logger.info("Starting proxy connection...")
            proxy_task = asyncio.create_task(self.relay())
    
            # Wait until either task fails or completes
            done, pending = await asyncio.wait(
                [local_server_task, proxy_task],
                return_when=asyncio.FIRST_EXCEPTION
            )
    
            # Cancel remaining tasks
            for task in pending:
                _ = task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    self.logger.info(f"Cancelled task: {task}")
    
            # Raise exception if any task failed
            for task in done:
                if task.exception():
                    raise task.exception()
    
        except Exception as e:
            self.logger.error(f"Server error: {e}")
            self.logger.error(traceback.format_exc())
            raise

    async def relay(self):
        """Maintain relay between API and MCP servers"""
    
        while self.running:
            try:
                self.logger.info("Attempting proxy connection...")
                await worker_relay(self.relay_id, self.handle_message)
                    
            except asyncio.CancelledError:
                self.logger.info("Proxy connection cancelled")
                break
            except Exception as e:
                self.logger.error(f"Proxy connection error: {e}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                if self.running:
                    self.logger.info("Retrying proxy connection in 5 seconds...")
                    await asyncio.sleep(5)

    async def handle_message(self, message: str):
        """Handle a message received from the inspector via the proxy."""
        try:
            # Parse the message (assuming it's JSON)
            data = cast(dict[str, Any], json.loads(message))
            
            method = cast(str, data.get('method', 'unknown'))
            self.logger.info(f"Processing inspector message type: {method}")
            
            # Forward to local MCP server
            response = await self.forward_to_local_server(data)
            
            # Check if this is a notification
            if method.startswith('notifications/'):
                self.logger.info(f"Notification {method} processed")
                # For notifications, DON'T send anything back through the queue
                # The inspector will handle the acknowledgment directly
                if response and response.get('error'):
                    self.logger.warning(f"Notification processing error: {response}")
                else:
                    self.logger.debug(f"Notification processed successfully")
                # Don't send anything to the queue for notifications
            else:
                # For regular requests, send the full response back to inspector
                await self.send_response(response)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse message as JSON: {e}")
            self.logger.error(f"Raw message: {repr(message)}")
            # Only send error responses for non-notifications
            if not data.get('method', '').startswith('notifications/'):
                await self.send_response({"error": f"Invalid JSON: {e}"})
        except Exception as e:
            self.logger.error(f"Error handling inspector message: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            # Only send error responses for non-notifications
            if not data.get('method', '').startswith('notifications/'):
                await self.send_response({"error": str(e)})

    async def forward_to_local_server(self, data):
        """Forward the request to the local MCP server."""
        try:
            # FastMCP in stateless HTTP mode expects the endpoint with trailing slash
            url = f"http://{self.host}:{self.port}/mcp/"
            
            # FastMCP stateless HTTP mode requires BOTH Accept headers
            headers = {
                "Content-Type": "application/json",
                "Accept": "text/event-stream, application/json",  # BOTH are required!
            }
            
            self.logger.info(f"Forwarding to local server: {url}")
            self.logger.debug(f"Request headers: {headers}")
            self.logger.debug(f"Request data: {json.dumps(data)[:200]}...")
            
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.post(
                    url,
                    json=data,
                    headers=headers,
                    timeout=30.0
                )
                
                self.logger.info(f"Local server response: {response.status_code}")
                self.logger.debug(f"Response Content-Type: {response.headers.get('content-type')}")
                
                # Handle successful responses (2xx status codes)
                if 200 <= response.status_code < 300:
                    content_type = response.headers.get('content-type', '')
                    
                    # For 202 Accepted (notifications), there might be no body or a simple acknowledgment
                    if response.status_code == 202:
                        self.logger.info("Received 202 Accepted (notification acknowledged)")
                        # Check if there's any response body
                        if response.text:
                            try:
                                # Try to parse as JSON
                                return response.json()
                            except:
                                # If not JSON, return a simple acknowledgment
                                return {"status": "accepted", "code": 202}
                        else:
                            # No body, return acknowledgment
                            return {"status": "accepted", "code": 202}
                    
                    # For 204 No Content, return an empty success
                    elif response.status_code == 204:
                        self.logger.info("Received 204 No Content")
                        return {"status": "success", "code": 204}
                    
                    # Handle 200 OK with content
                    elif response.status_code == 200:
                        # Handle SSE (Server-Sent Events) response
                        if 'text/event-stream' in content_type:
                            self.logger.info("Received SSE response, parsing...")
                            
                            # Parse SSE format
                            response_text = response.text
                            result = None
                            
                            for line in response_text.split('\n'):
                                if line.startswith('data: '):
                                    json_data = line[6:]  # Remove 'data: ' prefix
                                    if json_data.strip():
                                        try:
                                            result = json.loads(json_data)
                                            self.logger.debug(f"Parsed SSE data: {json.dumps(result)[:200]}...")
                                            break
                                        except json.JSONDecodeError as e:
                                            self.logger.warning(f"Failed to parse SSE line: {e}")
                            
                            if result:
                                return result
                            else:
                                self.logger.error(f"No valid JSON found in SSE response: {response_text[:500]}")
                                return {"error": "Failed to parse SSE response"}
                        
                        # Handle regular JSON response
                        elif 'application/json' in content_type:
                            try:
                                result = response.json()
                                self.logger.debug(f"Response: {json.dumps(result)[:200]}...")
                                return result
                            except Exception as e:
                                self.logger.error(f"Failed to parse JSON response: {e}")
                                self.logger.error(f"Response text: {response.text}")
                                return {"error": f"Invalid JSON response: {e}"}
                        
                        else:
                            self.logger.warning(f"Unexpected content type: {content_type}")
                            # Try to parse as JSON anyway
                            try:
                                result = response.json()
                                return result
                            except:
                                # Try to parse as SSE
                                for line in response.text.split('\n'):
                                    if line.startswith('data: '):
                                        try:
                                            return json.loads(line[6:])
                                        except:
                                            pass
                                # If no valid format found but status is 200, return success
                                if not response.text:
                                    return {"status": "success", "code": 200}
                                return {"error": f"Unknown response format: {response.text[:200]}"}
                    
                    # Handle other 2xx status codes
                    else:
                        self.logger.info(f"Received {response.status_code} response")
                        if response.text:
                            try:
                                return response.json()
                            except:
                                return {"status": "success", "code": response.status_code, "body": response.text}
                        else:
                            return {"status": "success", "code": response.status_code}
                
                elif response.status_code == 406:
                    error_msg = f"HTTP 406: {response.text}"
                    self.logger.error(error_msg)
                    return {"error": error_msg}
                
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text[:500]}"
                    self.logger.error(error_msg)
                    return {"error": error_msg}

        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return {"error": str(e)}

    async def send_response(self, data: object):
        """Send response back to proxy."""
        try:
            headers = {
                "X-Worker-ID": settings.WORKER_ID,
                "X-Relay-ID": str(self.relay_id),
                "Content-Type": "application/json"
            }
            
            content = json.dumps(data)
            
            async with httpx.AsyncClient(verify=False) as client:
                result = await client.post(
                    f"https://{settings.DJANGO_HOST}/v1/worker/{settings.WORKER_ID}/relay/",
                    content=content,
                    headers=headers
                )
                
            self.logger.info(f"Sent response to inspector: {result.status_code}")
            
        except Exception as e:
            self.logger.error(f"Error sending response to inspector: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
 
    @override
    def run(self):
        """Main process loop."""
        try:
            asyncio.run(self.run_server())
        except KeyboardInterrupt:
            self.logger.info("Process interrupted by user")
        except Exception as e:
            self.logger.error(f"Process error: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            # Don't re-raise here to prevent restart loop

    @override
    def _shutdown(self, signum: int, frame: Any):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

def main():
    process = MCPProcess()
    process.start()

if __name__ == "__main__":
    main()
