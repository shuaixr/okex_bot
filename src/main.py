import asyncio
from config import Config
from okex import OKEX
import pandas as pd
from ta.trend import ADXIndicator

from pandas.core.frame import DataFrame

from pmax import pmax


async def main():
    config = Config()
    await config.init()
    while True:
        await asyncio.gather(
            *[asyncio.create_task(task.run()) for task in config.task_list]
        )
        await asyncio.sleep(5)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
