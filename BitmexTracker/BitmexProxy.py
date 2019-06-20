import bitmex
import json
import pytz
import time
from datetime import datetime, timedelta
from BitmexTracker.DataManager import DataManager

class BitmexProxy(DataManager):

    class backtest():
        enabled = False
        timestep = None
        now = None

    @staticmethod
    def init_as_backtest(initial_time=datetime.now(pytz.utc) - timedelta(days=1), timestep=timedelta(seconds=1)):
        """

                :param initial_time: defaults to now() - 24h
                :param timestep: defaults to 1 second
                :return:
                """
        BitmexProxy.backtest.enabled = True
        BitmexProxy.backtest.timestep = timestep
        BitmexProxy.backtest.now = initial_time

    def now(self, freezed=False, *args, **kwargs):
        """
                Every call to datetime.now() must be replaced to this method to run backtest correctly.

                :param freezed: If False 'now' jumps to 'now + timestep' after call (timestep was passed to init_as_backtest)
                :return: backtest time or datetime.now()
                """
        if self.backtest.enabled:
            now = self.backtest.now
            if not freezed:
                self.tick()
                self.backtest.now += self.backtest.timestep
            return now
        return datetime.now(*args, **kwargs)

    class result_proxy(object):

        def __init__(self, real_bitmex):
            self.res = None
            self.real_bitmex = real_bitmex

        def result(self):
            return self.res

    class position_proxy(result_proxy):

        def __init__(self, *args, **kwargs):
            BitmexProxy.result_proxy.__init__(self, *args, **kwargs)

        def Position_updateLeverage(self, *args, **kwargs):

            """
                    Update leverage.

                    :param **kwargs: same as in bitmex api-connector
                    """

            if not BitmexProxy.backtest.enabled:
                return self.real_bitmex.Position.Position_updateLeverage(*args, **kwargs)
            self.res = None
            return self

        def Position_get(self, *args, **kwargs):

            """
                    Get positions.

                    :param **kwargs: same as in bitmex api-connector
                    """

            if not BitmexProxy.backtest.enabled:
                return self.real_bitmex.Position.Position_get(*args, **kwargs)


    class order_proxy(result_proxy):

        def __init__(self, *args, **kwargs):
            BitmexProxy.result_proxy.__init__(self, *args, **kwargs)
            self.orders = []

        def Order_new(self, *args, **kwargs):

            """
                    Place order.

                    :param **kwargs: same as in bitmex api-connector
                    """

            if not BitmexProxy.backtest.enabled:
                return self.real_bitmex.Order.Order_new(*args, **kwargs)
            self.orders.append(kwargs)
            self.res = None
            return self

        def Order_cancelAll(self, *args, **kwargs):

            """
                    Cancel all orders.

                    :param **kwargs: same as in bitmex api-connector
                    """

            if not BitmexProxy.backtest.enabled:
                return self.real_bitmex.Order.Order_cancelAll(*args, **kwargs)
            self.orders = []
            self.res = None
            return self

        def Order_getOrders(self, *args, **kwargs):

            """
                    Get orders that matches filter.

                    :param **kwargs: same as in bitmex api-connector
                    """

            if not BitmexProxy.backtest.enabled:
                return self.real_bitmex.Order.Order_getOrders(*args, **kwargs)
            if 'filter' in kwargs:
                conditions = json.loads(kwargs['filter'])
                self.res = [order for order in self.orders
                            if all(cond in order and order[cond] == conditions[cond] for cond in conditions)]
            self.res = self.orders
            return self

    class user_proxy(result_proxy):

        def __init__(self, *args, **kwargs):
            BitmexProxy.result_proxy.__init__(self, *args, **kwargs)
            self.balances = {}

        def User_getWalletSummary(self, *args, **kwargs):

            """
                    Get wallet summary.

                    :param **kwargs: same as in bitmex api-connector
                    """

            if not BitmexProxy.backtest.enabled:
                return self.real_bitmex.Order.Order_getOrders(*args, **kwargs)
            self.res = self.balances
            return self

    def __init__(self, *args, **kwargs):
        DataManager.__init__(self, **kwargs)
        self.real_bitmex = bitmex.bitmex(*args, **kwargs)
        self.Order = self.order_proxy(real_bitmex=self.real_bitmex)
        self.Position = self.position_proxy(real_bitmex=self.real_bitmex)
        self.User = self.user_proxy(real_bitmex=self.real_bitmex)

    def update_balance(self, side, market_data, amount):
        if side == 'Buy':
            price = market_data['buy']
            pass # TODO: update data returned by User_getWalletSummary
        else:
            price = market_data['sell']
            pass  # TODO: update data returned by User_getWalletSummary

    def execute_order(self, order, timestamp, market_data):

        if order['ordType'] == 'Market':
            self.update_balance(order['side'], market_data, order['orderQty'])
            self.logger.info('Market order executed {}'.format(timestamp))
            return
        elif order['ordType'] == 'Limit':
            pass
        elif order['ordType'] == 'Stop':
            pass
        elif order['ordType'] == 'StopLimit':
            pass
        elif order['ordType'] == 'MarketIfTouched':
            pass
        elif order['ordType'] == 'LimitIfTouched':
            pass
        elif order['ordType'] == 'MarketWithLeftOverAsLimit':
            pass
        elif order['ordType'] == 'Pegged':
            pass

    def tick(self):
        """
                Simulation tick. Runned every time DataManager method time() called if backtest is enabled

                :return: None
                """

        # get tickers, orderbooks and volumes between now and next timestamp
        tov = self.get_TOV(time_from=self.backtest.now, time_to=self.backtest.now + self.backtest.timestep)

        not_executed_orders = []

        # tick by tick scanning data
        for timestamp, market_data in tov.iterrows():
            not_executed_orders = [order for order in self.Order.orders
                               if (not order['open']) or (not self.execute_order(order, timestamp, market_data))]

        self.Order.orders = not_executed_orders

def main():
    # dm = DataManager(clean_cache=False)
    # df = dm.get_data_by_time(index='tickers', td=timedelta(hours=3))
    # all = dm.get_all_data(td=timedelta(hours=1), include_instrument=True)

    # df = dm.get_tickers(td=timedelta(hours=5), verbose=True)

    # all = dm.get_all_data(td=timedelta(hours=1))

    BitmexProxy.init_as_backtest()
    bp = BitmexProxy(clean_cache=False, test=True)
    # print(dm.now())
    # print(dm.now())
    # order = dm.Order.Order_new(symbol='XBTUSD', price=0)

    uncached_start = time.time()
    df = bp.get_candles(td=timedelta(hours=10), period='1min', verbose=True)
    uncached_end = time.time()
    print(df.head())

    cached_start = time.time()
    df = bp.get_candles(td=timedelta(hours=10), period='1min', verbose=True)
    cached_end = time.time()
    print(df.head())

    print('uncached time: {}s, cached time: {}s'.format(
        uncached_end - uncached_start,
        cached_end - cached_start
    ))

if __name__ == '__main__':
    main()