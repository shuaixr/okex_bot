import asyncio
import math
from typing import Union
from decimal import Decimal
from aiohttp import client
from pandas.core.frame import DataFrame
from ta.trend import ADXIndicator
from log import logger
import pandas as pd

from okex import (
    INST_TYPE_FUTURES,
    MGN_MODE_CROSS,
    OKEX,
    ORDER_TD_MODE_CROSS,
    ORDER_TYPE_LIMIT,
    POS_SIDE_LONG,
    POS_SIDE_SHORT,
    SIDE_BUY,
    SIDE_SELL,
)
from pmax import pmax

stm = {
    "5m": 3000000,
    "15m": 9000000,
    "30m": 18000000,
    "1H": 36000000,
    "2H": 7200000,
    "4H": 14400000,
}


def round_step_size(
    quantity: Union[float, Decimal], step_size: Union[float, Decimal]
) -> float:
    """Rounds a given quantity to a specific step size
    :param quantity: required
    :param step_size: required
    :return: decimal
    """
    precision: int = int(round(-math.log(step_size, 10), 0))
    front, behind = str(quantity).split(".")
    return float(front + "." + behind[0:precision])


class Task:
    def __init__(
        self, client: OKEX, id: int, min_margin: float, max_margin: float, bar: str
    ) -> None:
        self.client = client
        self.id = id
        self.min_margin = min_margin

        self.max_margin = max_margin
        self.bar = bar
        self.barms = stm[bar]
        self.logger = logger.getChild(f"Task({id}/{bar})")
        self.logger.debug("Task init")

        self.positions = None
        self.side_history = None
        self.ratio = 0.0
        pass

    async def asyncinit(self):
        self.instruments = (
            await self.client.get_instruments(INST_TYPE_FUTURES, None, self.id)
        )["data"][0]

    async def get_thousand_kline(self) -> DataFrame:
        client = self.client
        klines = pd.DataFrame(
            columns=(
                "Open Time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "VolumeCcy",
            )
        )
        before = None
        after = None
        for _ in range(10):
            candles = await client.candles(
                self.id,
                self.bar,
                after=after,
                before=before,
                limt=100,
            )
            if candles["code"] != "0":
                raise Exception("get_thousand_kline code not 0. " + str(candles))
            klines = klines.append(
                DataFrame(
                    pd.array(candles["data"]),
                    dtype=float,
                    columns=(
                        "Open Time",
                        "Open",
                        "High",
                        "Low",
                        "Close",
                        "Volume",
                        "VolumeCcy",
                    ),
                ),
                ignore_index=True,
            )
            after = (int)(klines.iloc[-1]["Open Time"])
            before = after - self.barms * 11

        klines = klines.loc[::-1].set_index(klines.index)

        klines["Open Time"] = pd.to_datetime(klines["Open Time"], unit="ms", utc=True)
        return klines

    def init_indicators(self, klines: DataFrame):
        klines["PMax"], klines["PMax_MA"], klines["PMax_dir"] = pmax(
            klines["High"], klines["Low"], klines["Close"], 10, 3, 10
        )
        adx = ADXIndicator(klines["High"], klines["Low"], klines["Close"])
        klines["adx"] = adx.adx()
        klines["adx_neg"] = adx.adx_neg()
        klines["adx_pos"] = adx.adx_pos()

    def count_ratio(self, klines: DataFrame, side: str) -> float:
        row = klines.iloc[-2]
        adx = row["adx"]
        adx_neg = row["adx_neg"]
        adx_pos = row["adx_pos"]
        ratio = (
            (
                (
                    adx
                    + (
                        adx_pos - adx_neg
                        if side == POS_SIDE_LONG
                        else adx_neg - adx_pos
                    )
                )
                / 2
            )
            if side != None
            else adx
        ) / 100
        return 0 if ratio < 0 else (1 if ratio > 1 else ratio)

    def count_sz(self, price: float, ctVal: float, lever: int) -> Union[int, float]:
        min_margin = self.min_margin
        max_margin = self.max_margin
        sz = (
            (self.ratio * (max_margin - min_margin) + min_margin)
            / price
            / ctVal
            * lever
        )
        minisz = (float)(self.instruments["minSz"])
        if sz < minisz:
            sz = minisz
        sz = round_step_size(sz, ((float)(self.instruments["lotSz"])))
        sz = int(sz) if sz.is_integer() else sz
        return sz

    def count_lever(self, min, max) -> int:

        lever = (int)(self.ratio * (max - min)) + min
        return min if lever < min else lever

    async def get_lever(self) -> int:
        return (int)(
            (await self.client.get_leverage_info(self.id, MGN_MODE_CROSS))["data"][0][
                "lever"
            ]
        )

    async def set_lever(self, lever: int):
        id = self.id
        client = self.client
        if (await self.get_lever()) != lever:
            d = await client.set_leverage(id, str(lever), MGN_MODE_CROSS)
            if d["code"] == "0":
                await self.refresh_positions()
                self.logger.debug(f"Change lever to {lever}")
            else:
                self.logger.warning(f"Change lever failed. Msg: {str(d)}")

    async def refresh_positions(self):
        d = await self.client.get_positions(INST_TYPE_FUTURES, self.id)
        if d["code"] != "0":
            self.logger.warning(f"refresh_positions failed. Msg: {str(d)}")
            return

        self.positions = d["data"][0]

    def get_side(self, klines: DataFrame) -> str:
        row2 = klines.iloc[-2]
        row3 = klines.iloc[-3]
        if (
            row3["PMax_dir"] != row2["PMax_dir"]
            and row2["Open Time"] != self.side_history
        ):
            self.side_history = row2["Open Time"]
            return POS_SIDE_LONG if row2["PMax_dir"] == 1 else POS_SIDE_SHORT
        return None

    async def get_price(self, side: str = None) -> int:
        return (float)(
            (await self.client.get_ticker(self.id))["data"][0][
                "bidPx"
                if side == SIDE_BUY
                else ("askPx" if side == SIDE_SELL else "last")
            ]
        )

    async def create_order_wait_filled(
        self,
        tdMode: str,
        side: str,
        ordType: str,
        sz: str,
        posSide: str = None,
        px: str = None,
    ) -> bool:

        client = self.client
        id = self.id
        self.logger.debug(
            f"Create order wait filled. id:{id}, tdMode:{tdMode}, side:{side}, ordType:{ordType}, sz:{sz}, posSide:{posSide}, px:{px}"
        )
        d = (await client.order(id, tdMode, side, ordType, sz, posSide, px))["data"][0]
        if d["sCode"] != "0":
            self.logger.warning(f"Create order failed. {str(d)}")
            return False
        orderid = d["ordId"]
        for _ in range(10):
            status = (await client.get_order(id, orderid))["data"][0]["state"]
            if status == "filled":
                return True
            await asyncio.sleep(3)
        self.logger.warning(f"Wait order filled timeout, Chanel order")
        d = (await client.cancel_order(id, orderid))["data"][0]
        if d["sCode"] != "0":
            self.logger.warning(f"Chanel order failed. {str(d)}")

        return False

    async def change_side(self, side: str):
        self.logger.debug(f"Change side to {side}")
        while 1:
            if self.positions["availPos"] != "":
                self.logger.debug("Close the reverse order")
                coside = (
                    SIDE_SELL
                    if self.positions["posSide"] == POS_SIDE_LONG
                    else SIDE_BUY
                )
                if await self.create_order_wait_filled(
                    ORDER_TD_MODE_CROSS,
                    coside,
                    ORDER_TYPE_LIMIT,
                    self.positions["availPos"],
                    self.positions["posSide"],
                    (await self.get_price(coside)),
                ):
                    break
                self.refresh_positions()
            else:
                break
        coside = SIDE_BUY if side == POS_SIDE_LONG else SIDE_SELL
        price = await self.get_price(coside)
        ctVal = (float)(self.instruments["ctVal"])
        lever = await self.get_lever()
        sz = self.count_sz(price, ctVal, lever)
        await self.create_order_wait_filled(
            ORDER_TD_MODE_CROSS,
            coside,
            ORDER_TYPE_LIMIT,
            sz,
            side,
            (await self.get_price(coside)),
        )

    async def change_sz(self):
        price = await self.get_price()
        ctVal = (float)(self.instruments["ctVal"])
        lever = await self.get_lever()
        sz = self.count_sz(price, ctVal, lever)
        availPos = self.positions["availPos"]
        if sz == availPos:
            return
        diff = sz - availPos
        self.logger.debug(f"Change sz {diff}.")
        coside = (
            (SIDE_SELL if diff < 0 else SIDE_BUY)
            if self.positions["posSide"] == POS_SIDE_LONG
            else (SIDE_BUY if diff < 0 else SIDE_SELL)
        )
        await self.create_order_wait_filled(
            ORDER_TD_MODE_CROSS,
            coside,
            ORDER_TYPE_LIMIT,
            abs(diff),
            self.positions["posSide"],
            (await self.get_price(coside)),
        )

    async def __run(self):

        await self.refresh_positions()

        klines = await self.get_thousand_kline()
        self.init_indicators(klines)

        side = self.get_side(klines)

        self.ratio = self.count_ratio(
            klines,
            (
                side
                if side != None
                else (
                    self.positions["posSide"]
                    if self.positions["availPos"] != ""
                    else None
                )
            ),
        )

        lever = self.count_lever(1, int(self.instruments["lever"]))
        await self.set_lever(lever=lever)
        if side != None:
            self.logger.debug(f"New side {side} at {self.side_history}")
            await self.change_side(side)
        elif self.positions["availPos"] != "":
            await self.change_sz()

    async def run(self):
        try:
            await self.__run()
        except Exception as e:
            self.logger.warning(e)
