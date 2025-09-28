from ..config import settings
from typing import AsyncGenerator
from meilisearch_python_sdk import AsyncClient
from contextlib import asynccontextmanager


@asynccontextmanager
async def async_get_search() -> AsyncGenerator[AsyncClient, None]:
    """
    Async context manager to get a Meilisearch client.

    Usage:
        async with get_meilisearch_client("http://localhost:7700", "your_api_key") as client:
            results = await client.index('products').search('laptop')
    """
    async_client = AsyncClient("http://localhost:7700", settings.MEILI_MASTER_KEY)
    try:
        yield async_client
    finally:
        # Add any cleanup code if needed
        pass

