import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import finetune.settings

def load_settings():
    return importlib.import_module("finetune.settings")

# Use variable annotation instead of cast
settings: "finetune.settings" = load_settings()  # type: ignore

print("Settings")
print(f"DJANGO_HOST: {settings.DJANGO_HOST}")
print(f"WORKER_ID: {settings.WORKER_ID}")
print(f"MCP_SERVER_PATH: {settings.MCP_SERVER_PATH}")

