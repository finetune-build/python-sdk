import aiohttp
import asyncio
import json
import time
from typing import Optional, Callable

class StreamingClient:
    def __init__(self, worker_id: str, base_url: str = "https://api.finetune.engineering"):
        self.worker_id = worker_id
        self.url = f"{base_url}/v1/worker/{worker_id}/streamable_http/"
        self.session: Optional[aiohttp.ClientSession] = None
        self.response: Optional[aiohttp.ClientResponse] = None
        self.write_queue: Optional[asyncio.Queue] = None
        self.reader_task: Optional[asyncio.Task] = None
        self._connected = False
        self.message_callback: Optional[Callable] = None
        self._stop_sending = asyncio.Event()  # <---- new event to signal generator to stop
        
    async def _message_generator(self):
        """Async generator that yields messages from the write queue"""
        # Send initial heartbeat
        yield (json.dumps({"type": "heartbeat", "message": "alive"}) + "\n").encode("utf-8")
        
        while not self._stop_sending.is_set():
            try:
                message = await asyncio.wait_for(self.write_queue.get(), timeout=1.0)
                if message is None:  # Sentinel value to stop
                    break
                yield (json.dumps(message) + "\n").encode("utf-8")
                self.write_queue.task_done()
            except asyncio.TimeoutError:
                # Optionally send heartbeat to keep connection alive
                yield (json.dumps({"type": "heartbeat", "message": "alive"}) + "\n").encode("utf-8")
            except asyncio.CancelledError:
                break
    
    async def connect(self, message_callback: Optional[Callable] = None):
        """Establish the streaming connection using async generator"""
        if self._connected:
            return
            
        self.message_callback = message_callback or self._default_message_handler
        self.session = aiohttp.ClientSession()
        self.write_queue = asyncio.Queue()
        self._stop_sending.clear()
        
        headers = {
            "Content-Type": "application/x-ndjson",
            "X-Worker-ID": self.worker_id,
        }
        
        # Start the connection with the streaming generator
        self.response = await self.session.post(
            self.url, 
            headers=headers, 
            data=self._message_generator(),
            ssl=False
        )
        
        if self.response.status != 200:
            error_text = await self.response.text()
            await self.close()
            raise Exception(f"Connection failed with status {self.response.status}: {error_text}")
        
        self._connected = True
        print(f"Connected to streaming endpoint. Status: {self.response.status}")
        
        # Start reading responses
        self.reader_task = asyncio.create_task(self._read_responses())
    
    async def _default_message_handler(self, message: dict):
        """Default message handler"""
        print(f"Received: {message}")
    
    async def _read_responses(self):
        """Read responses from the server"""
        try:
            async for line_bytes in self.response.content:
                line = line_bytes.decode().strip()
                if line:
                    try:
                        message = json.loads(line)
                        if self.message_callback:
                            if asyncio.iscoroutinefunction(self.message_callback):
                                await self.message_callback(message)
                            else:
                                self.message_callback(message)
                    except json.JSONDecodeError:
                        print(f"Failed to parse JSON: {line}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error reading responses: {e}")
        finally:
            # Server closed connection or error occurred: stop sending more data
            self._stop_sending.set()
            self._connected = False
    
    async def send_message(self, message: dict):
        """Send a message through the write stream"""
        if not self._connected or not self.write_queue:
            raise Exception("Not connected")
        await self.write_queue.put(message)
    
    async def send_heartbeat(self):
        """Send a heartbeat message"""
        await self.send_message({
            "type": "heartbeat", 
            "message": "alive", 
            "timestamp": time.time()
        })
    
    async def send_data(self, payload: dict):
        """Send a data message"""
        await self.send_message({
            "type": "data", 
            "payload": payload, 
            "timestamp": time.time()
        })
    
    async def close(self):
        """Close the connection and cleanup resources"""
        self._connected = False
        self._stop_sending.set()
        
        # Stop the message generator
        if self.write_queue:
            await self.write_queue.put(None)  # Sentinel to stop generator
        
        # Cancel background tasks
        if self.reader_task:
            self.reader_task.cancel()
            try:
                await self.reader_task
            except asyncio.CancelledError:
                pass
        
        # Close response and session
        if self.response:
            self.response.close()
        if self.session:
            await self.session.close()


# Your original working approach, but enhanced
class SimpleStreamingClient:
    def __init__(self, worker_id: str, base_url: str = "https://api.finetune.engineering"):
        self.worker_id = worker_id
        self.url = f"{base_url}/v1/worker/{worker_id}/streamable_http/"
        self.message_queue = asyncio.Queue()
        self._stop_generator = False
        
    async def message_generator(self):
        """Enhanced version of your gen_one_message that can send multiple messages"""
        # Send initial heartbeat (like your original)
        yield (json.dumps({"type": "heartbeat", "message": "alive"}) + "\n").encode("utf-8")
        
        # Then yield messages from queue
        while not self._stop_generator:
            try:
                message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                yield (json.dumps(message) + "\n").encode("utf-8")
                self.message_queue.task_done()
            except asyncio.TimeoutError:
                # Send periodic heartbeats when no messages
                yield (json.dumps({"type": "heartbeat", "message": "alive"}) + "\n").encode("utf-8")
            except asyncio.CancelledError:
                break
    
    async def connect_and_stream(self, message_callback=None):
        """Connect and handle the streaming (based on your working code)"""
        headers = {
            "Content-Type": "application/x-ndjson",
            "X-Worker-ID": self.worker_id,
        }
        
        callback = message_callback or (lambda msg: print(f"Received: {msg}"))
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, headers=headers, data=self.message_generator(), ssl=False) as resp:
                print("Status:", resp.status)
                async for line_bytes in resp.content:
                    line = line_bytes.decode().strip()
                    if line:
                        try:
                            message = json.loads(line)
                            if asyncio.iscoroutinefunction(callback):
                                await callback(message)
                            else:
                                callback(message)
                        except json.JSONDecodeError:
                            print("Received (raw):", line)
    
    async def send_message(self, message: dict):
        """Add a message to the send queue"""
        await self.message_queue.put(message)
    
    def stop(self):
        """Stop the message generator"""
        self._stop_generator = True

# Test functions
async def test_original_style():
    """Test using the enhanced version of your original approach"""
    print("=== Testing Original Style (Enhanced) ===")
    
    client = SimpleStreamingClient("3qssEZ8hV7tSPAMumoYhLu")
    
    async def handle_message(message):
        msg_type = message.get("type", "unknown")
        print(f"📨 [{msg_type}] {message}")
    
    # Start the connection in a background task
    connection_task = asyncio.create_task(client.connect_and_stream(handle_message))
    
    # Give it a moment to connect
    await asyncio.sleep(2)
    
    # Send some messages
    await client.send_message({"type": "test", "data": "Hello from client!"})
    await asyncio.sleep(1)
    
    await client.send_message({"type": "data", "payload": {"key": "value"}})
    await asyncio.sleep(1)
    
    await client.send_message({"type": "goodbye", "message": "Closing soon"})
    await asyncio.sleep(2)
    
    # Stop the client
    client.stop()
    
    # Wait a bit for final messages
    await asyncio.sleep(2)
    
    # Cancel the connection
    connection_task.cancel()
    try:
        await connection_task
    except asyncio.CancelledError:
        pass

async def test_advanced_client():
    """Test the more advanced streaming client"""
    print("\n=== Testing Advanced Client ===")
    
    async def handle_message(message):
        msg_type = message.get("type", "unknown")
        print(f"🔄 [{msg_type}] {message}")
    
    client = StreamingClient("3qssEZ8hV7tSPAMumoYhLu")
    
    try:
        await client.connect(handle_message)
        
        # Send some messages
        await client.send_heartbeat()
        await asyncio.sleep(1)
        
        await client.send_data({"test": "message 1"})
        await asyncio.sleep(1)
        
        await client.send_data({"test": "message 2"})
        await asyncio.sleep(2)
        
    finally:
        await client.close()

async def test_your_exact_original():
    """Your exact original code for comparison"""
    print("\n=== Your Original Code ===")
    
    async def gen_one_message():
        yield (json.dumps({"type": "heartbeat", "message": "alive"}) + "\n").encode("utf-8")

    url = "https://api.finetune.engineering/v1/worker/3qssEZ8hV7tSPAMumoYhLu/streamable_http/"
    headers = {
        "Content-Type": "application/x-ndjson",
        "X-Worker-ID": "3qssEZ8hV7tSPAMumoYhLu",
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=gen_one_message(), ssl=False) as resp:
            print("Status:", resp.status)
            async for line_bytes in resp.content:
                print("Received:", line_bytes.decode().strip())

if __name__ == "__main__":
    # Test your original code first to confirm it works
    asyncio.run(test_your_exact_original())
    
    # Then test the enhanced versions
    # asyncio.run(test_original_style())
    # asyncio.run(test_advanced_client())
