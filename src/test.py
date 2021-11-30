from okex import (
    INST_TYPE_FUTURES,
    OKEX,
    ORDER_TD_MODE_CROSS,
    ORDER_TYPE_LIMIT,
    POS_SIDE_LONG,
    SIDE_BUY,
)
import asyncio


async def main():
    ok = OKEX(
        "98ecee81-f665-4463-82c1-12949dba5da0",
        "DB1A2393D8463D910B856A64DF43199C",
        "20001025",
        True,
    )
    await ok.asyncinit()
    id = "BTC-USDT-220325"
    price = (await ok.get_ticker(id))["data"][0]["askPx"]
    d = await ok.get_positions(INST_TYPE_FUTURES, id)
    print(d["data"][0]["availPos"] == "")
    await ok.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
