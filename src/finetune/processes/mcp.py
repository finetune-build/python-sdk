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

from finetune.api.worker import get_worker_relay, post_worker_relay
from finetune.conf import settings
from finetune.mcp.relay import post_mcp_relay
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
                async for msg in get_worker_relay(self.relay_id):
                    # await self.handle_message(msg)
                    data = cast(dict[str, Any], json.loads(msg))
                    
                    method = cast(str, data.get('method', 'unknown'))
                    self.logger.info(f"Processing inspector message type: {method}")
                    
                    # Forward to local MCP server
                    # response = await self.forward_to_local_server(data)
                    response = await post_mcp_relay(self.host, self.port, None, data)

                    if response is not None:
                        mcp_session_id = response["headers"].get("mcp-session-id", None)
                        content = response["content"]
                        print(f"mcp_session_id: {mcp_session_id}")
                        print(f"content: {content}")
                        await post_worker_relay(self.relay_id, mcp_session_id, content)
                        # print("Got stream message:", msg)

            except asyncio.CancelledError:
                self.logger.info("Proxy connection cancelled")
                break
            except Exception as e:
                self.logger.error(f"Proxy connection error: {e}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                if self.running:
                    self.logger.info("Retrying proxy connection in 5 seconds...")
                    await asyncio.sleep(5)

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
