import aiohttp
import asyncio
from config import HEADERS

BASE_URL = "https://mpstats.io/api"

class MPStatsAPI:

    def __init__(self):
        self.timeout = aiohttp.ClientTimeout(total=60)

    async def get_categories(self):
        url = f"{BASE_URL}/oz/get/categories"

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url, headers=HEADERS) as response:
                response.raise_for_status()
                return await response.json()