from Functions.data_preparation import *
from Strategies.base import Strategy


class strategy_enhanced_fast(Strategy):
    def __init__(self, logger, timeperiod, rsiperiod = 7, rsilimit = 35, rsibars = 3, openbars = 1,
                 closebars = 1, openbody = 20, closebody = 50):
        Strategy.__init__(self, logger=logger, timeperiod=timeperiod)
        self.rsiperiod = rsiperiod
        self.rsilimit = rsilimit
        self.rsibars = rsibars
        self.openbars = openbars
        self.closebars = closebars
        self.openbody = openbody
        self.closebody = closebody

    def make_prediction(self, candles):
        self.rsi = talib.RSI(candles.close.values, self.rsiperiod)
        self.uplimit = 100 - self.rsilimit
        self.dnlimit = self.rsilimit

        rsidn = np.array(self.rsi < self.dnlimit, dtype=float)
        rsiup = np.array(self.rsi > self.uplimit, dtype=float)
        if self.rsibars > 1:
            rsidnok = talib.SMA(rsidn, self.rsibars) == 1
            rsiupok = talib.SMA(rsiup, self.rsibars) == 1
        else:
            rsidnok = rsidn == 1
            rsiupok = rsiup == 1

        body = np.abs(candles.close.values - candles.open.values)
        abody = talib.SMA(body, 10)

        self.openbodyok = body >= abody / 100 * self.openbody
        self.closebodyok = body >= abody / 100 * self.closebody

        bar = np.zeros_like(candles.close.values)

        bar[candles.close.values > candles.open.values] = 1
        bar[np.logical_and(candles.close.values <= candles.open.values,
                           candles.close.values < candles.open.values)] = -1
        bar[np.logical_and(candles.close.values <= candles.open.values,
                           candles.close.values >= candles.open.values)] = 0

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

        up = np.logical_and(np.logical_and(self.openrbarok, rsidnok), self.openbodyok)
        dn = np.logical_and(np.logical_and(self.opengbarok, rsiupok), self.openbodyok)

        if up[-1]:
            self.logger.info("Need long:")
            return 1
        if dn[-1]:
            self.logger.info("Need short:")
            return -1
        else:
            self.logger.info("Do nothing")
            return 0

    def need_exit(self, position_size):
        norma = (self.rsi[-1] > self.dnlimit and self.rsi[-1] < self.uplimit)
        exit = ((position_size > 0 and self.closegbarok[-1] and norma) or (
        position_size < 0 and self.closerbarok[-1] and norma)) and self.closebodyok[-1]
        return exit


