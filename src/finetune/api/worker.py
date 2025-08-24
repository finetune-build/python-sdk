import aiohttp

from uuid import UUID

from finetune.conf import settings
from finetune.api.utils import request

async def worker_relay(relay_id: UUID, handle_message):
    url = f"https://{settings.DJANGO_HOST}/v1/worker/{settings.WORKER_ID}/relay/"
    headers = {
        "X-Worker-ID": settings.WORKER_ID,
        "X-Relay-ID": str(relay_id),
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.get(url, ssl=False, timeout=None) as response:
                if response.status != 200:
                    print(f"Failed to connect to proxy: {response.status}")
                    return  # Exit; caller can handle reconnect

                buffer = ""
                async for chunk_bytes in response.content.iter_any():
                    chunk = chunk_bytes.decode()
                    if not chunk:
                        continue

                    buffer += chunk

                    # Process complete SSE messages
                    while "\n\n" in buffer:
                        message, buffer = buffer.split("\n\n", 1)
                        if message.startswith("data: "):
                            data = message[6:]  # Remove "data: " prefix
                            if data.strip() and data.strip() != "[DONE]":
                                await handle_message(data)

        except Exception as stream_error:
            print(f"Error in stream processing: {stream_error}")


async def worker_pong():
    url = f"https://{settings.DJANGO_HOST}/v1/worker/{settings.WORKER_ID}/pong/"
    headers = {
        "Authorization": f"Access {settings.ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.post(
                url, ssl=False, json={"worker_id": settings.WORKER_ID}
            ) as resp:
                if resp.status != 200:
                    print(f"Failed to respond to ping. Status: {resp.status}")
        except Exception as e:
            print(f"Ping response error: {e}")

async def worker_mcp_response(response, correlation_id):
    url = f"https://{settings.DJANGO_HOST}/v1/worker/{settings.WORKER_ID}/mcp/"
    headers = {
        "Authorization": f"Access {settings.ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    response = {
        "jsonrpc": "2.0",
        "result": response,
        "id": correlation_id,
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            async with session.post(
                url, ssl=False, json=response
            ) as resp:
                if resp.status != 200:
                    print(f"Failed to respond send response. Status: {resp.status}")
        except Exception as e:
            print(f"mcp response error: {e}")


async def get_worker_task_list(task_state="submitted", protocol="a2a"):
    return await request(
        "GET",
        f"worker/{settings.WORKER_ID}/task/?task_state={task_state}&protocol={protocol}"
    )

async def put_worker_task(worker_task_id, payload):
    """
    No nested updates, update status via websocket TaskStatusUpdateEvent.
    """
    return await request(
        "PUT",
        f"worker/{settings.WORKER_ID}/task/{worker_task_id}/",
        json=payload
    )
