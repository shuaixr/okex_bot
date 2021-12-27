import asyncio
import math
from re import sub
from typing import List, Union
from decimal import Decimal
from aiohttp import client
from pandas.core.frame import DataFrame
from ta.trend import ADXIndicator
from log import logger
import pandas as pd
import traceback
from okex import (
    INST_TYPE_SWAP,
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
    "2H": 72000000,
    "4H": 144000000,
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


def pmaxdir_to_posside(dir: int) -> str:
    return POS_SIDE_LONG if dir == 1 else POS_SIDE_SHORT


class Task:
    def __init__(
        self,
        client: OKEX,
        inst_type: str,
        id: int,
        min_margin: float,
        max_margin: float,
        bar: str,
        mal: int,
        atrm: int,
        sub_sz_ratio: float,
        avg_adx_ratio: List[str],
        candles_lock: asyncio.Lock,
    ) -> None:
        self.client = client
        self.inst_type = inst_type
        self.id = id
        self.min_margin = min_margin
        self.max_margin = max_margin
        self.bar = bar
        self.mal = mal
        self.atrm = atrm
        self.sub_sz_ratio = sub_sz_ratio
        self.avg_adx_ratio = avg_adx_ratio
        self.candles_lock = candles_lock

        self.barms = stm[bar]

        self.logger = logger.getChild(f"Task({id}/{bar}/mal({mal}))")
        self.logger.debug("Task init")

        self.positions = None
        self.ratio = 0.0
        self.klines_cache = pd.DataFrame(
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
        self.indicators_cache: DataFrame = None
        self.indicators_cache_time = None
        self.last_sub_sz_time = 0.0
        pass

    async def asyncinit(self):
        self.instruments = (
            await self.client.get_instruments(self.inst_type, None, self.id)
        )["data"][0]

    async def candles(
        self,
        instId: str,
        bar: str = None,
        after: str = None,
        before: str = None,
        limt: int = 100,
    ):
        async with self.candles_lock:
            candles = await self.client.candles(
                instId=instId, bar=bar, after=after, before=before, limt=limt
            )
            await asyncio.sleep(0.1)
        return candles

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
        end = None
        if len(self.klines_cache.index) != 0:
            end = (int)(self.klines_cache.iloc[2]["Open Time"])
        before = None
        after = None
        for _ in range(10):

            if end != None and before != None and before < end:
                klines = klines.append(self.klines_cache, ignore_index=True)
                klines = klines.drop_duplicates(subset=["Open Time"], ignore_index=True)
                break
            candles = await self.candles(
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

            # self.logger.debug(f"Get k{str(klines)}")
            after = (int)(klines.iloc[-1]["Open Time"])
            before = after - (self.barms * 11)
        if len(klines.index) > 1000:
            klines = klines.head(1000)
        # logger.debug(f"{self.id}\n{str(klines)}")
        self.klines_cache = klines
        klines = klines.loc[::-1].set_index(klines.index)

        klines["Open Time"] = pd.to_datetime(klines["Open Time"], unit="ms", utc=True)
        return klines

    def init_adx_indicators(self, klines: DataFrame) -> DataFrame:
        adx = ADXIndicator(klines["High"], klines["Low"], klines["Close"], window=28)
        klines["adx"] = adx.adx()
        klines["adx_neg"] = adx.adx_neg()
        klines["adx_pos"] = adx.adx_pos()
        return klines

    def init_indicators(self, klines: DataFrame) -> DataFrame:
        last_ot = klines.iloc[-2]["Open Time"]
        if self.indicators_cache_time == last_ot:
            return self.indicators_cache
        klines["PMax"], klines["PMax_MA"], klines["PMax_dir"], klines["hl2"] = pmax(
            klines["High"], klines["Low"], klines["Close"], 100, self.atrm, self.mal
        )
        klines = self.init_adx_indicators(klines)

        self.indicators_cache_time = last_ot
        self.indicators_cache = klines
        return klines

    def count_ratio(self, klines: DataFrame, side: str) -> float:
        row = klines.iloc[-2]

        adx = row["adx"]
        adx_neg = row["adx_neg"]
        adx_pos = row["adx_pos"]
        side_neg_pos_diff = (
            adx_pos - adx_neg if side == POS_SIDE_LONG else adx_neg - adx_pos
        )
        """
        if side_neg_pos_diff < 0:
            side_neg_pos_diff *= 2
        adxnp2 = ((adx + side_neg_pos_diff) / 2) if side != None else adx
        
        ratio = adxnp2 / 100
        """
        ratio = 0.0
        ratio = side_neg_pos_diff * 2 / 100
        return 0 if ratio < 0 else (1 if ratio > 1 else ratio)

    async def count_avg_ratio(self, klines: DataFrame, side: str) -> float:
        ratio_list: List[float] = []
        ratio_list.append(self.count_ratio(klines, side))
        for bar in self.avg_adx_ratio:
            d = await self.candles(instId=self.id, bar=bar, limt=100)
            if d["code"] != "0":
                raise Exception("count_avg_ratio get kline code not 0. " + str(d))
            klines = DataFrame(
                pd.array(d["data"]),
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
            )
            klines = klines.loc[::-1].set_index(klines.index)
            klines = self.init_adx_indicators(klines)
            ratio_list.append(self.count_ratio(klines, side))
        rll = len(ratio_list)
        ratio_sum = 0.0
        for ratio in ratio_list:
            ratio_sum += ratio

        return ratio_sum / rll

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
                self.logger.debug(f"Change lever to {lever}, ratio: {self.ratio}")
            else:
                self.logger.warning(f"Change lever failed. Msg: {str(d)}")

    async def refresh_positions(self):
        d = await self.client.get_positions(self.inst_type, self.id)
        if d["code"] == "51030" and self.inst_type == INST_TYPE_SWAP:
            return
        if d["code"] != "0":
            self.logger.warning(f"refresh_positions failed. Msg: {str(d)}")
            return
        if len(d["data"]) == 0:
            self.positions = {
                "adl": "",
                "availPos": "",
                "avgPx": "",
                "cTime": "",
                "ccy": "",
                "deltaBS": "",
                "deltaPA": "",
                "gammaBS": "",
                "gammaPA": "",
                "imr": "",
                "instId": "",
                "instType": "",
                "interest": "0",
                "last": "",
                "lever": "",
                "liab": "",
                "liabCcy": "",
                "liqPx": "",
                "margin": "",
                "markPx": "",
                "mgnMode": "",
                "mgnRatio": "",
                "mmr": "",
                "notionalUsd": "",
                "optVal": "",
                "pos": "",
                "posCcy": "",
                "posId": "",
                "posSide": "",
                "thetaBS": "",
                "thetaPA": "",
                "tradeId": "",
                "uTime": "",
                "upl": "",
                "uplRatio": "",
                "usdPx": "",
                "vegaBS": "",
                "vegaPA": "",
            }
        else:
            self.positions = d["data"][0]

    def get_side(self, klines: DataFrame) -> str:
        row2 = klines.iloc[-2]
        row3 = klines.iloc[-3]
        side = None
        if row3["PMax_dir"] != row2["PMax_dir"]:
            side = pmaxdir_to_posside(row2["PMax_dir"])
        if side == self.positions["posSide"]:
            side = None
        return side

    async def get_price(self, side: str = None) -> int:
        ticksz = (float)(self.instruments["tickSz"])
        ticker = (await self.client.get_ticker(self.id))["data"][0]

        bid = (float)(ticker["bidPx"])
        ask = (float)(ticker["askPx"])
        last = (float)(ticker["last"])
        if side == None:
            return last
        ab = ask - bid
        ab2 = ab / 2
        price = bid if side == SIDE_BUY else ask
        if ab > (ticksz * 2):
            price = price + ab2 if side == SIDE_BUY else price - ab2
        return round_step_size((price + last) / 2, ticksz)

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
        for _ in range(5):
            await asyncio.sleep(1)

            status = (await client.get_order(id, orderid))["data"][0]["state"]
            if status == "filled":
                return True
        self.logger.warning(f"Wait order filled timeout, Chanel order")
        d = (await client.cancel_order(id, orderid))["data"][0]
        if d["sCode"] != "0":
            self.logger.warning(f"Chanel order failed. {str(d)}")

        return False

    async def change_side(self, side: str):
        self.logger.debug(f"Change side to {side}")
        while True:
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
                await self.refresh_positions()
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

    async def sub_sz(self, klines: DataFrame):
        row = klines.iloc[-2]

        posside = self.positions["posSide"]
        if pmaxdir_to_posside(row["PMax_dir"]) != posside:
            return
        opents = pd.Timestamp(row["Open Time"]).timestamp()
        if opents <= self.last_sub_sz_time and self.last_sub_sz_time != 0:
            return
        hl2 = row["hl2"]
        pm = row["PMax"]
        if posside == POS_SIDE_LONG and hl2 > pm:
            return
        if posside == POS_SIDE_SHORT and hl2 < pm:
            return
        availPos = float(self.positions["availPos"])
        avgPx = float(self.positions["avgPx"])
        minsz = float(self.instruments["minSz"])
        subsz = 0.0
        if (posside == POS_SIDE_LONG and hl2 < avgPx) or (
            posside == POS_SIDE_SHORT and hl2 > avgPx
        ):
            subsz = availPos * self.sub_sz_ratio
        else:
            subsz = availPos * abs(hl2 - pm) / abs(pm - avgPx)
            logger.debug(f"SubSZ by ratio {abs(hl2 - pm) / abs(pm - avgPx)}")

        subsz = minsz if subsz < minsz else subsz
        subsz = availPos - minsz if availPos - subsz < minsz else subsz
        subsz = round_step_size(subsz, step_size=float(self.instruments["lotSz"]))
        if subsz <= 0:
            return
        subsz = int(subsz) if subsz.is_integer() else subsz
        self.logger.debug(f"Sub sz {subsz}")
        coside = SIDE_SELL if posside == POS_SIDE_LONG else SIDE_BUY

        if await self.create_order_wait_filled(
            ORDER_TD_MODE_CROSS,
            coside,
            ORDER_TYPE_LIMIT,
            subsz,
            self.positions["posSide"],
            (await self.get_price(coside)),
        ):
            self.last_sub_sz_time = opents

    async def __run(self):

        await self.refresh_positions()

        klines = await self.get_thousand_kline()
        klines = self.init_indicators(klines)

        side = self.get_side(klines)

        if side != None:
            self.ratio = await self.count_avg_ratio(
                klines,
                side,
            )
            lever = self.count_lever(1, int(self.instruments["lever"]))
            await self.set_lever(lever=lever)
            self.logger.debug(f"New side {side}")
            await self.change_side(side)
        elif self.positions["availPos"] != "":
            await self.sub_sz(klines)

    async def run(self):
        try:
            await self.__run()
        except Exception as e:
            self.logger.warning(str(e) + str(traceback.format_exc()))
