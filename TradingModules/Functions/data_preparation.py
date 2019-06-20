import datetime
from collections import namedtuple

import ccxt
import numpy as np
import pandas as pd
import talib
from Functions.consts import *


def resample(dataframe, to_timeframe):
    ohlc_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }

    resampled = dataframe.resample(to_timeframe).agg(ohlc_dict)

    return resampled


def get_last_close(timeframe):
    # bitmex connector
    bitmex_api = ccxt.bitmex({
    })
    # params:
    symbol = 'BTC/USD'
    limit = 1
    params = {'partial': False}
    comission_rate = 0.0

    since = bitmex_api.milliseconds() - limit * 60 * 1000
    candles = bitmex_api.fetch_ohlcv(symbol, timeframe, since, limit, params)
    df = pd.DataFrame(candles, columns=[
                      'timestamp', 'open', 'high', 'low', 'close', 'volume'])

    return df.iloc[-1].close


def get_last_candles(timeframe='1h'):
    # bitmex connector
    bitmex_api = ccxt.bitmex({
    })
    # params:
    symbol = 'BTC/USD'
    limit = 500
    params = {'partial': False}

    since = bitmex_api.milliseconds() - limit * 60 * 60 * 1000
    candles = bitmex_api.fetch_ohlcv(symbol, timeframe, since, limit, params)
    df = pd.DataFrame(candles, columns=[
                      'timestamp', 'open', 'high', 'low', 'close', 'volume'])

    df.timestamp = [datetime.datetime.fromtimestamp(item / 1000.0) for item in df.timestamp.values]

    df.set_index(df.timestamp, inplace=True)

    df.drop('timestamp', axis=1, inplace=True)

    return df


def get_data(timeframe):

    # Usage example
    # dm = DataManager()
    # df = dm.get_data_by_time(index='tickers', td=timedelta(hours=3))
    # df = dm.get_all_data(td=timedelta(minutes=1))
    # df = dm.get_candles_by_time(td=timedelta(hours=1), period='1min')

    # bitmex connector
    bitmex_api = ccxt.bitmex({
        'apiKey': BITMEX_API_KEY,
        'secret': BITMEX_API_SECRET,
    })
    # params:
    symbol = 'BTC/USD'
    limit = 500
    params = {'partial': False}
    comission_rate = 0.0

    since = bitmex_api.milliseconds() - limit * 60 * 60 * 1000
    candles = bitmex_api.fetch_ohlcv(symbol, timeframe, since, limit, params)
    df = pd.DataFrame(candles, columns=[
                      'timestamp', 'open', 'high', 'low', 'close', 'volume'])

    # data transforming
    df = add_HA(df)
    df = add_indicators(df)

    columns = ['open', 'high', 'low', 'close', 'volume',
               'HA_Close', 'HA_Open', 'HA_High', 'HA_Low']
    for column in columns:
        for i in range(1, 4):
            df[column + str(i)] = df[column].shift(i)
    df.dropna(inplace=True)

    df["perc_change"] = 1 / (df['HA_Close'].pct_change(-1) + 1)
    df["perc_change_sign"] = np.sign(df["perc_change"] - 1 - comission_rate)
    df = df[~(df["perc_change_sign"] == 0)]

    return df


def data_percentage_change(df):
    perc_change = df.perc_change
    perc_change_sign = df.perc_change_sign

    perc_df = df / df.shift(1)
    perc_df.perc_change = perc_change
    perc_df.perc_change_sign = perc_change_sign

    perc_df = perc_df.iloc[1:]
    perc_df.drop(columns=['aroondown', 'aroonup'], inplace=True)

    return perc_df


def train_test_split(df):
    # preparing for training and prediction
    df = data_percentage_change(df)

    train = df.head(-1)
    test = df.tail(1)

    X_train = train.drop(
        columns=["perc_change", "perc_change_sign", "timestamp"])

    y_train = train["perc_change_sign"]

    X_test = test.drop(
        columns=["perc_change", "perc_change_sign", "timestamp"])

    return X_train, X_test, y_train


def add_HA(df):
    '''Heiken Ashi transform.
    input:
        DF with OHLC columns.
    output:
        DF with OHLC and new Heiken Ashi OHLC columns.
    '''
    df['HA_Close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4

    nt = namedtuple('nt', ['open', 'close'])
    previous_row = nt(df.ix[0, 'open'], df.ix[0, 'close'])

    for i, row in enumerate(df.itertuples()):
        ha_open = (previous_row.open + previous_row.close) / 2
        df.ix[i, 'HA_Open'] = ha_open
        previous_row = nt(ha_open, row.close)

    df['HA_High'] = df[['HA_Open', 'HA_Close', 'high']].max(axis=1)
    df['HA_Low'] = df[['HA_Open', 'HA_Close', 'low']].min(axis=1)
    return df


def add_indicators(df):
    high = df["HA_High"].values
    close = df["HA_Close"].values
    low = df["HA_Low"].values
    _open = df["HA_Open"].values
    volume = df["volume"].values.astype('uint32')

    df["APO"] = talib.APO(close, fastperiod=9, slowperiod=21, matype=0)
    df["APO"] = talib.APO(close, fastperiod=9, slowperiod=21, matype=0)
    df["aroondown"], df["aroonup"] = talib.AROON(high, low, timeperiod=14)
    df["BOP"] = talib.BOP(_open, high, low, close)
    df["CCI"] = talib.CCI(high, low, close, timeperiod=10)
    df["DX"] = talib.DX(high, low, close, timeperiod=10)
    df["MOM"] = talib.MOM(close, timeperiod=10)
    df["slowk"], df["slowd"] = talib.STOCH(high, low, close, fastk_period=5, slowk_period=3, slowk_matype=0,
                                           slowd_period=3, slowd_matype=0)
    df["OBV"] = talib.OBV(close, np.asarray(volume, dtype='float'))
    df["ADOSC"] = talib.ADOSC(high, low, close, np.asarray(
        volume, dtype='float'), fastperiod=3, slowperiod=10)
    df["upperband"], df["middleband"], df["lowerband"] = talib.BBANDS(close, timeperiod=5, nbdevup=2, nbdevdn=2,
                                                                      matype=0)

    return df


def write_state(filename, state):
    with open(filename, 'a') as f:
        f.write(str(state) + '\n')
