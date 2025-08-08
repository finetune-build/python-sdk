import asyncio
import ssl
import aiohttp
from uuid import uuid4

DOMAIN = "developmentpackage.finetune.engineering"
STREAM_PATH = "/v1/worker/3qssEZ8hV7tSPAMumoYhLu/streamable_http/"

async def async_stream_client(initialization_id: str):
    print("Starting worker stream client...")
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    reader, writer = await asyncio.open_connection(
        'developmentpackage.finetune.engineering',
        443,
        ssl=ssl_context,
    )
    
    mcp_session_id = str(uuid4())
    print(f"mcp_session_id: {mcp_session_id}")
    
    # Send GET request to establish streaming connection
    request = (
        "GET /v1/worker/3qssEZ8hV7tSPAMumoYhLu/streamable_http/ HTTP/1.1\r\n"
        "Host: developmentpackage.finetune.engineering\r\n"
        "Connection: keep-alive\r\n"
        "Accept: text/event-stream\r\n"
        f"Initialization-Id: {initialization_id}\r\n"
        f"Mcp-Session-Id: {mcp_session_id}\r\n"
        "\r\n"
    )
    writer.write(request.encode())
    await writer.drain()
    
    # Read response headers
    headers = b""
    while True:
        line = await reader.readline()
        headers += line
        if line == b"\r\n":
            break
    
    headers_text = headers.decode()
    print("[Worker] Response headers:\n", headers_text)
    
    # Send initial pong using separate POST request
    print("Sending initial pong...")
    await send_message(mcp_session_id, "data: pong from python-sdk\n\n")
    
    # Now read the stream and respond to pings
    while True:
        line = await reader.readline()
        if not line:
            print("[Worker] ❌ Connection closed by server")
            break
        
        decoded = line.decode(errors="ignore").strip()
        if decoded:
            print(f"[Worker] 🔄 Received: {decoded}")
            if "ping" in decoded:
                # Send pong response via separate POST
                await send_message(mcp_session_id, "data: pong from python-sdk\n\n")

async def send_message(mcp_session_id: str, message: str):
    """Send a message to the inspector via a separate POST request"""
    print(f"[Worker] Sending message: {message}")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                f"https://{DOMAIN}{STREAM_PATH}",
                data=message,
                headers={
                    "Content-Type": "text/plain",
                    "mcp-session-id": mcp_session_id,
                },
                ssl=False,
            ) as resp:
                print(f"[Worker] Message sent. Status: {resp.status}")
                if resp.status != 204:
                    text = await resp.text()
                    print(f"[Worker] Server response: {text}")
        except Exception as e:
            print(f"[Worker] Error sending message: {e}")

# Run the client
if __name__ == "__main__":
    # You'll need to get the initialization_id from somewhere
    # (e.g., passed as an argument or from the worker task)
    initialization_id = "YOUR_INITIALIZATION_ID_HERE"
    asyncio.run(async_stream_client(initialization_id))
