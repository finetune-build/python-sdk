import aiohttp
import asyncio
import json
import time

class WebSocketClient:
    def __init__(self, worker_id, base_url="ws://api.finetune.engineering"):
        # Note: Use ws:// instead of wss:// when disabling SSL
        self.worker_id = worker_id
        self.url = f"{base_url}/ws/worker/{worker_id}/streamable_http/"
        self.session = None
        self.ws = None
        self.receive_task = None
    
    async def connect(self):
        self.session = aiohttp.ClientSession()
        self.ws = await self.session.ws_connect(self.url, ssl=False)  # Disable SSL verification here
        print(f"Connected to WebSocket {self.url}")
        self.receive_task = asyncio.create_task(self._receive_messages())
    
    async def _receive_messages(self):
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    print(f"Received: {data}")
                except json.JSONDecodeError:
                    print(f"Received non-JSON: {msg.data}")
            elif msg.type == aiohttp.WSMsgType.ERROR:
                print(f"WebSocket error: {self.ws.exception()}")
                break
    
    async def send_json(self, data):
        if self.ws is None:
            raise RuntimeError("WebSocket not connected")
        await self.ws.send_json(data)
    
    async def send_heartbeat(self):
        await self.send_json({
            "type": "heartbeat",
            "message": "alive",
            "timestamp": time.time()
        })
    
    async def send_data(self, payload):
        await self.send_json({
            "type": "data",
            "payload": payload,
            "timestamp": time.time()
        })
    
    async def close(self):
        if self.ws:
            await self.ws.close()
        if self.session:
            await self.session.close()
        if self.receive_task:
            self.receive_task.cancel()
            try:
                await self.receive_task
            except asyncio.CancelledError:
                pass

# Example usage
async def main():
    client = WebSocketClient("3qssEZ8hV7tSPAMumoYhLu")
    await client.connect()
    
    await client.send_heartbeat()
    await asyncio.sleep(1)
    
    await client.send_data({"test": "message 1"})
    await asyncio.sleep(1)
    
    await client.send_data({"test": "message 2"})
    await asyncio.sleep(5)
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())

