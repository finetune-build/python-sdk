import aiohttp

async def post_mcp_relay(host: str, port: int, mcp_session_id: str, data):
    url = f"http://{host}:{port}/mcp/"
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream, application/json",
    }
    if mcp_session_id:
        headers["mcp-session-id"] = mcp_session_id

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=data, headers=headers) as response:
            return {
                "headers": response.headers,
                "content": await response.read(),
            }

