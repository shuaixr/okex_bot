import asyncio
from task import Task
from typing import List
import yaml
from okex import OKEX
from log import logger

Config = None


class Config:
    def __init__(self):
        self.task_list: List[Task] = []

    async def init(self):
        await self.refresh_config()

        await asyncio.gather(
            *[asyncio.create_task(task.asyncinit()) for task in self.task_list]
        )

    async def refresh_config(self):

        with open("config.yaml", "r") as stream:
            try:
                self.config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                logger.error(exc)
        exchange_config = self.config["api"]
        client = OKEX(
            api_key=exchange_config["key"],
            api_secretkey=exchange_config["secretkey"],
            api_passphrase=exchange_config["passphrase"],
            testnet=exchange_config["testnet"]
        )
        await client.asyncinit()
        self.client = client

        for item in self.config["task_list"]:
            get_local_or_global_config = lambda s: item.get(s, self.config.get(s))
            task = Task(
                client=client,
                id=item["id"],
                min_margin=item["min_margin"],
                max_margin=item["max_margin"],
                bar=get_local_or_global_config("bar"),
            )
            self.task_list.append(task)
