from Functions.data_preparation import *
from Strategies.base import Strategy

class strategy_mas_extreme(Strategy):

    def __init__(self, logger, timeperiod, fastlen=5, slowlen=22, bars=2, openbars = 1,
                 closebars = 1, openbody = 20, closebody = 50):
        Strategy.__init__(self, logger=logger, timeperiod=timeperiod)
        self.fastlen = fastlen
        self.slowlen = slowlen
        self.bars = bars
        self.prev_trend = 0

        self.openbars = openbars
        self.closebars = closebars
        self.openbody = openbody
        self.closebody = closebody

    def make_prediction(self, candles):

        #candles = resample(candles, self.timeperiod)

        lasthigh = candles.rolling(self.slowlen)['close'].max()
        lastlow = candles.rolling(self.slowlen)['close'].min()
        center = (lasthigh + lastlow) / 2

        lasthigh2 = candles.rolling(self.fastlen)['close'].max()
        lastlow2 = candles.rolling(self.fastlen)['close'].min()
        center2 = (lasthigh2 + lastlow2) / 2

        trend = 1 if candles.low.values[-1] > center.values[-1] and candles.low.values[-2] > center.values[-2] \
            else -1 if candles.high.values[-1] < center.values[-1] and candles.high.values[-2] < center.values[-2] \
            else self.prev_trend

        self.prev_trend = trend

        bar = np.zeros_like(candles.close.values)

        bar[candles.close.values > candles.open.values] = 1
        bar[np.logical_and(candles.close.values <= candles.open.values,
                           candles.close.values < candles.open.values)] = -1
        bar[np.logical_and(candles.close.values <= candles.open.values,
                           candles.close.values >= candles.open.values)] = 0

        #filters
        body = np.abs(candles.close.values - candles.open.values)
        abody = talib.SMA(body, 10)

        self.openbodyok = body >= abody / 100 * self.openbody
        self.closebodyok = body >= abody / 100 * self.closebody

        gbar = np.array(bar == 1, dtype=float)
        rbar = np.array(bar == -1, dtype=float)

        if self.openbars > 1:
            self.opengbarok = talib.SMA(gbar, self.openbars) == 1
            self.openrbarok = talib.SMA(rbar, self.openbars) == 1
        else:
            self.opengbarok = gbar == 1
            self.openrbarok = rbar == 1

        if self.closebars > 1:
            self.closegbarok = talib.SMA(gbar, self.closebars) == 1
            self.closerbarok = talib.SMA(rbar, self.closebars) == 1
        else:
            self.closegbarok = gbar == 1
            self.closerbarok = rbar == 1

        if self.bars == 0:
            redbars = 1
        elif self.bars == 1 and bar[-1] == -1:
            redbars = 1
        elif self.bars == 2 and bar[-1] == -1 and bar[-2] == -1:
            redbars = 1
        elif self.bars == 3 and bar[-1] == -1 and bar[-2] == -1 and bar[-3] == -1:
            redbars = 1
        else:
            redbars = 0

        if self.bars == 0:
            greenbars = 1
        elif self.bars == 1 and bar[-1] == 1:
            greenbars = 1
        elif self.bars == 2 and bar[-1] == 1 and bar[-2] == 1:
            greenbars = 1
        elif self.bars == 3 and bar[-1] == 1 and bar[-2] == 1 and bar[-3] == 1:
            greenbars = 1
        else:
            greenbars = 0

        up = 1 if trend == 1 and (candles.low.values[-1] < center2.values[-1]) \
                  and (redbars == 1) and self.openbodyok[-1] and self.openrbarok[-1] else 0
        dn = 1 if trend == -1 and (candles.high.values[-1] > center2.values[-1]) \
                  and (greenbars == 1) and self.openbodyok[-1] and self.opengbarok[-1] else 0

        up2 = 1 if candles.high.values[-1] < center.values[-1] and candles.high.values[-1] < center2.values[-1] \
                   and bar[-1] == -1 and self.openbodyok[-1] and self.openrbarok[-1] else 0
        dn2 = 0 if candles.low.values[-1] > center.values[-1] and candles.low.values[-1] > center2.values[-1] \
                   and bar[-1] == 1 and self.openbodyok[-1] and self.opengbarok[-1] else 0

        if up == 1 or up2 == 1:
            self.logger.info("Need long:")
            return 1
        if dn == 1:
            self.logger.info("Need short:")
            return -1
        else:
            self.logger.info("Do nothing")
            return 0


