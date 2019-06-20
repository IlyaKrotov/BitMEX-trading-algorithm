from Functions.data_preparation import *
from Strategies.base import Strategy

class strategy_fast_rsi(Strategy):

    def __init__(self, logger, timeperiod, rsiperiod1=9, rsilimit1=19, rsiperiod2=14, rsilimit2=22):
        Strategy.__init__(self, logger=logger, timeperiod=timeperiod)
        self.rsiperiod1 = rsiperiod1
        self.rsilimit1 = rsilimit1
        self.rsiperiod2 = rsiperiod2
        self.rsilimit2 = rsilimit2

    def make_prediction(self, candles):

        #candles = resample(candles, self.timeperiod)

        rsi1 = talib.RSI(candles.close.values, self.rsiperiod1)
        rsi2 = talib.RSI(candles.close.values, self.rsiperiod2)

        uplimit1 = 100 - self.rsilimit1
        dnlimit1 = self.rsilimit1

        uplimit2 = 100 - self.rsilimit2
        dnlimit2 = self.rsilimit2

        self.body = np.abs(candles.close.values - candles.open.values)
        self.abody = talib.SMA(self.body, 10)

        self.bar = np.zeros_like(candles.close.values)

        self.bar[candles.close.values > candles.open.values] = 1
        self.bar[np.logical_and(candles.close.values <= candles.open.values, candles.close.values < candles.open.values)] = -1
        self.bar[np.logical_and(candles.close.values <= candles.open.values, candles.close.values >= candles.open.values)] = 0

        up1 = np.logical_and(np.logical_and(self.bar == -1, rsi1 < dnlimit1),
                            (self.body > self.abody / 5))

        dn1 = np.logical_and(np.logical_and(self.bar == 1, rsi1 > uplimit1),
                            (self.body > self.abody / 5))

        up2 = np.logical_and(np.logical_and(self.bar == -1, rsi2 < dnlimit2),
                            (self.body > self.abody / 5))

        dn2 = np.logical_and(np.logical_and(self.bar == 1, rsi2 > uplimit2),
                            (self.body > self.abody / 5))

        self.norma = np.logical_and(np.logical_and(np.logical_and(rsi1 > dnlimit1, rsi1 < uplimit1),
                                                   rsi2 > dnlimit2),
                                    rsi2 < uplimit2)

        self.needup = np.logical_or(up1, up2)
        self.needdn = np.logical_or(dn1, dn2)

        if self.needup[-1]:
            self.logger.info("Need long:")
            return 1
        if self.needdn[-1]:
            self.logger.info("Need short:")
            return -1
        else:
            self.logger.info("Do nothing")
            return 0

    def need_exit(self, position_size):
        return (((position_size > 0 and self.bar[-1] == 1 and self.norma[-1]) or
               (position_size < 0 and self.bar[-1] == -1 and self.norma[-1])) and
                self.body[-1] > self.abody[-1] / 2)


