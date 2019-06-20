import bitmex
import time
import ccxt
import os
import pandas as pd
import json

class Trader():

    def __init__(self, logger, strategy, bitmex_params, leverage, symbol, num_of_positions,
                 max_num_of_positions, trade_market, deposit_percent, new_trade_to_average_percent, simpleOrderQty=True):
        self.client = bitmex.bitmex(**bitmex_params)
        self.strategy = strategy
        self.logger = logger

        self.leverage = leverage
        self.symbol = symbol
        self.deposit_percent = deposit_percent
        self.new_trade_to_average_percent = new_trade_to_average_percent

        self.simpleOrderQty = simpleOrderQty
        self.set_tradable_qty()

        self.max_number_of_positions = max_num_of_positions
        self.number_of_positions = num_of_positions
        self.trade_market = trade_market

        if trade_market:
            self.logger.info("Trade by market")
        else:
            self.logger.info("Trade by close prices")

        # state of the trader
        self.open_position = 0  # money
        self.executed_prices = []
        self.executed_qts = []

    def save_state(self, filename):
        with open('../states/' + filename + '.state', 'w') as f:
            json.dump(self.state_to_dict(), f)

    def load_state(self, filename):
        if not os.path.exists('../states/'):
            os.makedirs('../states/')
        if not os.path.exists('../states/' + filename + '.state'):
            self.number_of_positions = 0
            self.executed_prices = []
            self.executed_qts = []
            self.save_state(filename)
        else:
            with open('../states/' + filename + '.state', 'r') as f:
                self.state_from_dict(json.load(f))

    def state_to_dict(self):
        return {
            'number_of_positions': self.number_of_positions,
            'executed_prices': self.executed_prices,
            'executed_qts': self.executed_qts
        }

    def state_from_dict(self, d):
        self.number_of_positions = d['number_of_positions']
        self.executed_prices = d['executed_prices']
        self.executed_qts = d['executed_qts']

    def set_tradable_qty(self):
        total, _ = self.balances_status()
        self.tradable_qty = float(total*self.deposit_percent*self.leverage)
        self.logger.info(f"New tradable quantity {self.tradable_qty}")
        time.sleep(0.5)

    def last_mark_price(self):
        return float(self.client.Trade.Trade_getBucketed(symbol=self.symbol, binSize='1m', partial=True,
                                                         count=1, reverse=True).result()[0][0]['close'])

    def update_open_position(self):
        res = self.client.Position.Position_get().result()
        if len(res[0]) != 0:
            self.open_position = int(res[0][0]['currentQty'])
        else:
            self.open_position = 0
        time.sleep(0.5)

    def get_last_order_info(self):
        def get_info():
            result = self.client.Order.Order_getOrders(
                symbol='XBTUSD', count=1, reverse=True
            ).result()[0][0]
            return result

        done = False
        while not done:
            try:
                result = get_info()

                if self.simpleOrderQty:
                    qty, price = result['orderQty'], result['price']
                else:
                    qty, price = result['orderQty'], result['price']

                done = True
                return qty, price
            except Exception:
                self.logger.info("503 response, another try to get last order info")
                time.sleep(1)
                self.logger.info(Exception)


    def buy_market_with_leverage(self, quantity, leverage):
        def make_order():
            if self.simpleOrderQty:
                response = self.client.Order.Order_new(
                    symbol=self.symbol,
                    side="Buy",
                    simpleOrderQty=quantity,
                ).result()
            else:
                response = self.client.Order.Order_new(
                    symbol=self.symbol,
                    side="Buy",
                    orderQty=quantity,
                ).result()
            return response

        done = False
        while not done:
            try:
                response = make_order()
                done = True
            except Exception:
                self.logger.info("503 response, another try to place an order")
                time.sleep(1)
                self.logger.info(Exception)

        def leverage_set():
            response = self.client.Position.Position_updateLeverage(
                symbol=self.symbol, leverage=leverage
            ).result()
            return response

        done = False
        while not done:
            try:
                response = leverage_set()
                done = True
            except Exception:
                self.logger.info("503 response, another try to set leverage")
                time.sleep(1)
                self.logger.info(Exception)


    def sell_market_with_leverage(self, quantity, leverage):
        def make_order():
            if self.simpleOrderQty:
                response = self.client.Order.Order_new(
                    symbol=self.symbol,
                    side="Sell",
                    simpleOrderQty=quantity,
                ).result()
            else:
                response = self.client.Order.Order_new(
                    symbol=self.symbol,
                    side="Sell",
                    orderQty=quantity,
                ).result()
            return response

        done = False
        while not done:
            try:
                response = make_order()
                done = True
            except Exception:
                self.logger.info("503 response, another try to place an order")
                time.sleep(1)
                self.logger.info(Exception)

        def leverage_set():
            response = self.client.Position.Position_updateLeverage(
                symbol=self.symbol, leverage=leverage
            ).result()
            return response

        done = False
        while not done:
            try:
                response = leverage_set()
                done = True
            except Exception:
                self.logger.info("503 response, another try to set leverage")
                time.sleep(1)
                self.logger.info(Exception)

    def balances_status(self):
        wallet_summary = self.client.User.User_getWalletSummary().result()[0]

        total = 0
        realised_pnl = 0

        for item in wallet_summary:
            if item['transactType'] == 'Total':
                total = item['walletBalance'] / 100000000.
            if item['transactType'] == 'RealisedPNL':
                realised_pnl = item['unrealisedPnl'] / 100000000.

        return float(total), float(realised_pnl)

    def buy_with_leverage(self, price, quantity, leverage):
        def make_order():
            if self.simpleOrderQty:
                response = self.client.Order.Order_new(
                    price=price,
                    symbol=self.symbol,
                    side="Buy",
                    simpleOrderQty=quantity,
                ).result()
            else:
                response = self.client.Order.Order_new(
                    price=price,
                    symbol=self.symbol,
                    side="Buy",
                    orderQty=quantity,
                ).result()
            return response

        self.logger.info(f"Buy sell price: {price}")

        done = False
        while not done:
            try:
                response = make_order()
                done = True
            except Exception:
                self.logger.info("503 response, another try to place an order")
                time.sleep(1)
                self.logger.info(Exception)

        def leverage_set():
            response = self.client.Position.Position_updateLeverage(
                symbol=self.symbol, leverage=leverage
            ).result()
            return response

        done = False
        while not done:
            try:
                response = leverage_set()
                done = True
            except Exception:
                self.logger.info("503 response, another try to set leverage")
                time.sleep(1)
                self.logger.info(Exception)

    def sell_with_leverage(self, price, quantity, leverage):
        def make_order():
            if self.simpleOrderQty:
                response = self.client.Order.Order_new(
                    price=price,
                    symbol=self.symbol,
                    side="Sell",
                    simpleOrderQty=quantity,
                ).result()
            else:
                response = self.client.Order.Order_new(
                    price=price,
                    symbol=self.symbol,
                    side="Sell",
                    orderQty=quantity,
                ).result()
            return response

        self.logger.info(f"Close sell price: {price}")

        done = False
        while not done:
            try:
                response = make_order()
                done = True
            except Exception:
                self.logger.info("503 response, another try to place an order")
                time.sleep(1)
                self.logger.info(Exception)

        def leverage_set():
            response = self.client.Position.Position_updateLeverage(
                symbol=self.symbol, leverage=leverage
            ).result()
            return response

        done = False
        while not done:
            try:
                response = leverage_set()
                done = True
            except Exception:
                self.logger.info("503 response, another try to set leverage")
                time.sleep(1)
                self.logger.info(Exception)

    def cancel_all_orders(self):
        return self.Order.Order_cancelAll().result()

    def close_all_orders(self, side):
        return self.client.Order.Order_new(
            symbol=self.symbol,
            execInst="Close"
        ).result()

    def get_last_close(self, timeframe):
        bitmex_api = ccxt.bitmex({})
        symbol = 'BTC/USD'
        limit = 1
        params = {'partial': False}

        since = bitmex_api.milliseconds() - limit * 60 * 1000
        candles = bitmex_api.fetch_ohlcv(symbol, timeframe, since, limit, params)
        df = pd.DataFrame(candles, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume'])

        return df.iloc[-1].close