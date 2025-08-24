import os
from dotenv import load_dotenv

_ = load_dotenv()

DJANGO_HOST: str = os.environ.get("DJANGO_HOST", "api.finetune.build")
BROKER: str = os.environ.get("FTW_BROKER_URL", "sqla+sqlite:///celery_broker.sqlite")
BACKEND: str = os.environ.get("FTW_CELERY_BACKEND_URL", "db+sqlite:///celery_results.sqlite")

# TODO: Add in error handling for no worker id and access token
WORKER_ID: str = os.environ.get("FINETUNE_WORKER_ID", "")
ACCESS_TOKEN: str = os.environ.get("FINETUNE_ACCESS_TOKEN", "")

# TODO: Remove completely, worker should not need to know this value
# HOST = os.environ.get("FINETUNE_HOST")
MCP_SERVER_PATH = os.environ.get("MCP_SERVER_PATH")

__all__ = ["WORKER_ID", "DJANGO_HOST", "BROKER", "BACKEND", "ACCESS_TOKEN", "MCP_SERVER_PATH"]
