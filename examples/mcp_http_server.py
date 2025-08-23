from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.prompts import base
from mcp.types import Completion, ResourceTemplateReference, PromptReference

# Create a stateless MCP server
mcp = FastMCP(name="SimpleServer", stateless_http=True)

# Completion
@mcp.completion()
async def handle_completion(ref: PromptReference | ResourceTemplateReference | None, argument, context):
    if isinstance(ref, ResourceTemplateReference):
        return Completion(values=["user1", "user2"])
    if isinstance(ref, PromptReference):
        return Completion(values=["prompt1", "prompt2"])
    return None

# Prompts
@mcp.prompt()
def review_code(code: str) -> str:
    return f"Please review this code:\n\n{code}"

@mcp.prompt()
def debug_error(error: str) -> list[base.Message]:
    return [
        base.UserMessage("I'm seeing this error:"),
        base.UserMessage(error),
        base.AssistantMessage("I'll help debug that. What have you tried so far?"),
    ]

# Resources 
@mcp.resource("info://server")
def server_info() -> str:
    """Get server information"""
    print("called Simple MCP Server v1.0")
    return "Simple MCP Server v1.0"

@mcp.resource("config://app")
def get_config() -> str:
    """Static configuration data"""
    return "App configuration here"

# Resource Templates
@mcp.resource("users://{user_id}/profile")
def get_user_profile(user_id: str) -> str:
    """Dynamic user data"""
    return f"Profile data for user {user_id}"

# Tools
@mcp.tool(description="Echo a message back")
def echo(message: str) -> str:
    """Echo the provided message"""
    print(f"Echo: {message}")
    return f"Echo: {message}"

@mcp.tool(description="Add two numbers")
def add(a: int, b: int) -> int:
    """Add two numbers together"""
    print(f"Adding {a} + {b}")
    result = a + b
    print(f"Result: {result}")
    return result

