import asyncio
import ssl

async def async_stream_client():
    print("called")

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    reader, writer = await asyncio.open_connection(
        'developmentpackage.finetune.engineering',
        443,
        ssl=ssl_context,
    )

    request = (
        "GET /v1/worker/3qssEZ8hV7tSPAMumoYhLu/streamable_http/ HTTP/1.1\r\n"
        "Host: developmentpackage.finetune.engineering\r\n"
        "Connection: keep-alive\r\n"
        "Accept: text/event-stream\r\n"
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
    print(headers.decode())

    # Now read the stream
    while True:
        line = await reader.readline()
        if not line:
            print("connection closed")
            break
        print(f"🔄 {line.decode().strip()}")

asyncio.run(async_stream_client())




# import asyncio
# import ssl
# import aiohttp
# import re
# import uuid
#
# DOMAIN = "developmentpackage.finetune.engineering"
# STREAM_PATH = "/v1/worker/3qssEZ8hV7tSPAMumoYhLu/streamable_http/"
#
# session_id = None  # Global so POSTs can use it later
#
#
# async def listen_for_messages():
#     print("called")
#     global session_id
#
#     ssl_context = ssl.create_default_context()
#     ssl_context.check_hostname = False
#     ssl_context.verify_mode = ssl.CERT_NONE
#
#     reader, writer = await asyncio.open_connection(
#         'developmentpackage.finetune.engineering',
#         443,
#         ssl=ssl_context,
#     )
#
#     request = (
#         "GET /v1/worker/3qssEZ8hV7tSPAMumoYhLu/streamable_http/ HTTP/1.1\r\n"
#         "Host: developmentpackage.finetune.engineering\r\n"
#         "Connection: keep-alive\r\n"
#         "Accept: text/event-stream\r\n"
#         "\r\n"
#     )
#     writer.write(request.encode())
#     await writer.drain()
#
#     # Read response headers
#     headers = b""
#     while True:
#         line = await reader.readline()
#         headers += line
#         if line == b"\r\n":
#             break
#
#     headers_text = headers.decode()
#     print("[Worker] Response headers:\n", headers_text)
#
#     # Extract mcp-session-id from headers
#     match = re.search(r"mcp-session-id:\s*(\S+)", headers_text, re.IGNORECASE)
#     if match:
#         session_id = match.group(1)
#         print(f"[Worker] ✅ Got session ID from server: {session_id}")
#     else:
#         # No session ID received, generate one
#         session_id = str(uuid.uuid4())
#         print(f"[Worker] ⚠️ No session ID received. Generated one: {session_id}")
#
#     # Read SSE stream
#     while True:
#         line = await reader.readline()
#         if not line:
#             print("[Worker] ❌ Connection closed by server")
#             break
#         decoded = line.decode(errors="ignore").strip()
#         if decoded:
#             print(f"[Worker] 🔄 Received: {decoded}")
#             if "ping" in decoded:
#                 await send_pong(decoded)
#
#
# async def send_pong(ping_msg: str):
#     global session_id
#     if not session_id:
#         print("[Worker] ❌ Cannot send pong: no session_id yet")
#         return
#
#     print("[Worker] 🔁 Sending pong...")
#     async with aiohttp.ClientSession() as session:
#         async with session.post(
#             f"https://{DOMAIN}{STREAM_PATH}",
#             data="data: pong from worker\n\n",
#             headers={
#                 "Content-Type": "text/plain",
#                 "mcp-session-id": session_id,
#             },
#             ssl=False,
#         ) as resp:
#             print(f"[Worker] ✅ Pong sent. Status: {resp.status}")
#             text = await resp.text()
#             print(f"[Worker] Server response: {text}")
#
#
# async def main():
#     await listen_for_messages()
#
# if __name__ == "__main__":
#     asyncio.run(main())
#
