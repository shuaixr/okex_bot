from pandas import Series, DataFrame
from ta.volatility import AverageTrueRange
from ta.trend import EMAIndicator


def pmax(
    high: Series,
    low: Series,
    close: Series,
    atr_length: int,
    atr_multiplier: float,
    ma_length: int,
):

    valpha = 2 / (ma_length + 1)

    df = DataFrame()
    df["High"] = high

    df["Close"] = close
    df["Low"] = low
    df["atr"] = AverageTrueRange(high, low, close, atr_length).average_true_range()
    df["src"] = 0.0
    df["MA"] = 0.0
    df["dir"] = 1
    df["longStop"] = None
    df["shortStop"] = None

    df["PMax"] = None
    for index in range(0, len(df.index)):

        row = df.iloc[index]
        row1 = df.iloc[index - 1]

        src = (row["High"] + row["Low"]) / 2
        df.at[index, "src"] = src
        if index < 1:
            continue
        df.at[index, "vud1"] = src - row1["src"] if src > row1["src"] else 0
        df.at[index, "vdd1"] = row1["src"] - src if src < row1["src"] else 0

        if index < 9:
            continue
        vUD = 0.0
        for i in range(0, 9):
            vUD += df.iloc[index - i]["vud1"]
        df.at[index, "vUD"] = vUD
        vDD = 0.0
        for i in range(0, 9):
            vDD += df.iloc[index - i]["vdd1"]
        df.at[index, "vDD"] = vDD
        vCMO = (vUD - vDD) / (vUD + vDD)
        df.at[index, "CMO"] = vCMO
        VAR = (valpha * abs(vCMO) * src) + (1 - valpha * abs(vCMO)) * row1["MA"]
        df.at[index, "MA"] = VAR
        longStop = VAR - atr_multiplier * row["atr"]
        longStopPrev = longStop if row1["longStop"] == None else row1["longStop"]
        longStop = max(longStopPrev, longStop) if VAR > longStopPrev else longStop
        df.at[index, "longStop"] = longStop
        shortStop = VAR + atr_multiplier * row["atr"]
        shortStopPrev = shortStop if row1["shortStop"] == None else row1["shortStop"]
        shortStop = min(shortStopPrev, shortStop) if VAR < shortStopPrev else shortStop
        df.at[index, "shortStop"] = shortStop

        dir = row1["dir"]
        dir = (
            1
            if dir == -1 and VAR > shortStopPrev
            else (-1 if dir == 1 and VAR < longStopPrev else dir)
        )
        df.at[index, "dir"] = dir
        Pmax = longStop if dir == 1 else shortStop
        df.at[index, "PMax"] = Pmax

    return df["PMax"], df["MA"], df["dir"]
