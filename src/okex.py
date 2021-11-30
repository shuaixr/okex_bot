from enum import Enum
import json
from os import fork
from urllib.parse import urljoin, urlencode
import hmac
import asyncio
import aiohttp
from datetime import date, datetime
from aiohttp import http
from asyncio.exceptions import LimitOverrunError
from requests.models import HTTPError
import base64
import copy

# from log import logger


class ClientError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


ORDER_TD_MODE_ISOLATED = "isolated"
ORDER_TD_MODE_CROSS = "cross"
ORDER_TD_MODECASH = "cash"

MGN_MODE_CROSS = "cross"
MGN_MODE_ISOLATED = "isolated"

SIDE_BUY = "buy"
SIDE_SELL = "sell"


POS_SIDE_LONG = "long"
POS_SIDE_SHORT = "short"
POS_SIDE_NET = "net"

ORDER_TYPE_MARKET = "market"
ORDER_TYPE_LIMIT = "limit"
ORDER_TYPE_POST_ONLY = "post_only"
ORDER_TYPE_FOK = "fok"
ORDER_TYPE_IOC = "ioc"
ORDER_TYPE_OPTIMAL_LIMIT_IOC = "optimal_limit_ioc"


INST_TYPE_SPOT = "SPOT"
INST_TYPE_MARGIN = "MARGIN"
INST_TYPE_SWAP = "SWAP"
INST_TYPE_FUTURES = "FUTURES"
INST_TYPE_OPTION = "OPTION"


class OKEX:
    __API_METHOD_GET = "GET"
    __API_METHOD_POST = "POST"
    __API_METHOD_DELETE = "DELETE"

    __SECURITY_TYPE_PUBLIC = "PUBLIC"
    __SECURITY_TYPE_PRIVATE = "PRIVATE"

    def __init__(
        self,
        api_key: str = None,
        api_secretkey: str = None,
        api_passphrase: str = None,
        testnet: bool = False,
    ) -> None:
        self.api_key = api_key
        self.api_secretkey = api_secretkey
        self.api_passphrase = api_passphrase
        self.testnet = testnet

    async def asyncinit(self) -> None:
        self.http = aiohttp.ClientSession()

    async def close(self) -> None:
        await self.http.close()

    async def get_leverage_info(self, instId: str, mgnMode: str):
        return await self.__api(
            self.__API_METHOD_GET,
            "/api/v5/account/leverage-info",
            {"instId": instId, "mgnMode": mgnMode},
            self.__SECURITY_TYPE_PRIVATE,
        )

    async def cancel_order(self, instId: str, ordId: str = None, clOrdId: str = None):
        return await self.__api(
            self.__API_METHOD_POST,
            "/api/v5/trade/cancel-order",
            {"instId": instId, "ordId": ordId, "clOrdId": clOrdId},
            self.__SECURITY_TYPE_PRIVATE,
        )

    async def set_leverage(
        self,
        instId: str,
        lever: str,
        mgnMode: str,
        posSide: str = POS_SIDE_NET,
        ccy: str = None,
    ):
        return await self.__api(
            self.__API_METHOD_POST,
            "/api/v5/account/set-leverage",
            {
                "instId": instId,
                "ccy": ccy,
                "lever": lever,
                "posSide": posSide,
                "mgnMode": mgnMode,
            },
            self.__SECURITY_TYPE_PRIVATE,
        )

    async def get_orders_history(self):
        return await self.__api(
            self.__API_METHOD_GET,
            "/api/v5/trade/orders-history",
            {},
            self.__SECURITY_TYPE_PRIVATE,
        )

    async def get_ticker(self, instId: str):
        return await self.__api(
            self.__API_METHOD_GET,
            "/api/v5/market/ticker",
            {"instId": instId},
            self.__SECURITY_TYPE_PUBLIC,
        )

    async def order(
        self,
        instId: str,
        tdMode: str,
        side: str,
        ordType: str,
        sz: str,
        posSide: str = None,
        px: str = None,
    ):
        return await self.__api(
            self.__API_METHOD_POST,
            "/api/v5/trade/order",
            {
                "instId": instId,
                "tdMode": tdMode,
                "side": side,
                "ordType": ordType,
                "sz": sz,
                "posSide": posSide,
                "px": px,
            },
            self.__SECURITY_TYPE_PRIVATE,
        )

    async def get_order(self, instId: str = None, ordId: str = None):
        return await self.__api(
            self.__API_METHOD_GET,
            "/api/v5/trade/order",
            {"instId": instId, "ordId": ordId},
            self.__SECURITY_TYPE_PRIVATE,
        )

    async def get_positions(
        self, instType: str = None, instId: str = None, posId: str = None
    ):
        return await self.__api(
            self.__API_METHOD_GET,
            "/api/v5/account/positions",
            {"instType": instType, "instId": instId, "posId": posId},
            self.__SECURITY_TYPE_PRIVATE,
        )

    async def candles(
        self,
        instId: str,
        bar: str = None,
        after: str = None,
        before: str = None,
        limt: int = 100,
    ):
        return await self.__api(
            self.__API_METHOD_GET,
            "/api/v5/market/candles",
            {
                "instId": instId,
                "bar": bar,
                "after": after,
                "before": before,
                "limt": limt,
            },
            self.__SECURITY_TYPE_PUBLIC,
        )

    async def get_instruments(self, instType: str, uly: str = None, instId: str = None):
        return await self.__api(
            self.__API_METHOD_GET,
            "/api/v5/public/instruments",
            {"instType": instType, "uly": uly, "instId": instId},
            self.__SECURITY_TYPE_PUBLIC,
        )

    async def ticker(self, instId: str):
        return await self.__api(
            self.__API_METHOD_GET,
            "/api/v5/market/ticker",
            {"instId": instId},
            self.__SECURITY_TYPE_PUBLIC,
        )

    async def __api(self, method: str, urlpath: str, param: dict, security_type: str):
        param = {k: v for k, v in param.items() if v is not None}

        baseurl = "https://www.okex.com/"
        url = urljoin(baseurl, urlpath)
        headers = {}
        if self.testnet:
            headers["x-simulated-trading"] = "1"
        if security_type != self.__SECURITY_TYPE_PUBLIC:

            headers["CONTENT-TYPE"] = "application/json"
            headers["OK-ACCESS-TIMESTAMP"] = datetime.utcnow().isoformat()[:-3] + "Z"
            headers["OK-ACCESS-PASSPHRASE"] = self.api_passphrase
            headers["OK-ACCESS-KEY"] = self.api_key

            if method != self.__API_METHOD_POST:

                headers["OK-ACCESS-SIGN"] = (
                    headers["OK-ACCESS-TIMESTAMP"]
                    + method
                    + urlpath
                    + ("" if param == {} else "?")
                    + urlencode(param)
                )
            else:
                headers["OK-ACCESS-SIGN"] = (
                    headers["OK-ACCESS-TIMESTAMP"]
                    + method
                    + urlpath
                    + json.dumps(param)
                )
            headers["OK-ACCESS-SIGN"] = base64.b64encode(
                hmac.new(
                    bytes(self.api_secretkey, encoding="utf-8"),
                    bytes(headers["OK-ACCESS-SIGN"], encoding="utf-8"),
                    digestmod="sha256",
                ).digest()
            ).decode("utf-8")

        param = None if param == {} else param
        headers = None if headers == {} else headers
        async with self.http.request(
            method=method,
            url=url,
            params=(param if method != self.__API_METHOD_POST else None),
            data=(json.dumps(param) if method == self.__API_METHOD_POST else None),
            headers=headers,
        ) as r:
            text = await r.text()
            try:
                r.raise_for_status()
            except Exception as e:
                raise ClientError(e, text)
            return json.loads(text)
