from okex import (
    INST_TYPE_FUTURES,
    MGN_MODE_CROSS,
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
    id = "DOT-USDT-220325"
    print(
        await ok.candles(
            id,
            "5m",
            limt=100,
        )
    )
    await ok.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
