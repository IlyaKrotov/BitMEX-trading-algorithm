import time
import numpy as np
from Traders.base import Trader

# https://www.bitmex.com/api/explorer/

class trader_enhanced_fast(Trader):

    def __init__(self, logger, strategy, bitmex_params, deposit_percent, leverage, new_trade_to_average_percent, max_num_of_positions=5, num_of_positions=0, trade_market=False):
        Trader.__init__(self, logger=logger, strategy=strategy, bitmex_params=bitmex_params,
                        leverage=leverage, symbol='XBTUSD', deposit_percent=deposit_percent,
                        max_num_of_positions=max_num_of_positions, num_of_positions=num_of_positions,
                        new_trade_to_average_percent=new_trade_to_average_percent, trade_market=trade_market)

        self.logger.info("Initialization ended.")

    def exec_trade(self, candles):
        # TODO: закупаться не по маркету, а по нужной цене. Но тогда надо отслеживать выполнение ордеров.
        self.logger.info("Started trade execution")
        decision = self.strategy.make_prediction(candles=candles)
        needExit = self.strategy.need_exit(position_size=float(self.open_position))
        self.logger.info("Decision - {}".format(decision))

        if decision == 0 and not needExit:
            self.logger.info("Do nothing")
            self.logger.info("Need exit: {}".format(needExit))
            time.sleep(1)
            return
        elif decision == 0 and needExit:
            if self.open_position > 0:
                self.logger.info("Selling open positions, exit is needed")
                self.close_all_orders('SELL')
                self.executed_prices = []
                self.executed_qts = []
                self.number_of_positions = 0
                #change tradable amount
                self.set_tradable_qty()
            elif self.open_position < 0:
                self.logger.info("Buying open positions, exit is needed")
                self.close_all_orders('BUY')
                self.executed_prices = []
                self.executed_qts = []
                self.number_of_positions = 0
                #change tradable amount
                self.set_tradable_qty()

        self.update_open_position()
        #if self.open_position == 0.0:
            #self.logger.info("Change tradable_qty to - {}".format(self.tradable_qty))
            #self.tradable_qty = int(self.balances_status()[0]/self.max_number_of_positions)

        self.logger.info("Current open position - {}".format(self.open_position))
        self.logger.info("Need exit: {}".format(needExit))


        # надо закрывать позиции если открыты, а решение противоположное стороне открытых позиций
        if self.open_position > 0 and (decision == -1):
            self.logger.info("Selling open positions")
            self.close_all_orders('SELL')
            self.executed_prices = []
            self.executed_qts = []
            self.number_of_positions = 0
            #change tradable amount
            self.set_tradable_qty()

        if self.open_position < 0 and (decision == 1):
            self.logger.info("Buying open positions")
            self.close_all_orders('BUY')
            self.executed_prices = []
            self.executed_qts = []
            self.number_of_positions = 0
            # change tradable amount
            self.set_tradable_qty()

        # настало время закупиться!
        if decision == -1:
            self.logger.info("Sell market")
            # выполненых ордеров у нас нет, значит это должен быть первый
            if self.number_of_positions == 0:
                self.logger.info("First order")

                if self.trade_market:
                    self.sell_market_with_leverage(self.tradable_qty, self.leverage)
                else:
                    self.sell_with_leverage(float(self.get_last_close('1h')), self.tradable_qty, self.leverage)

                qty, price = self.get_last_order_info()
                self.logger.info("Executed qty {}, price {}".format(qty, price))

                self.number_of_positions = 1
                self.executed_prices.append(float(price))
                print(self.executed_prices)
                #self.executed_qts.append(float(qty))
            else:
                self.logger.info("Average sell price: {}".format(np.mean(self.executed_prices)))
                # это уже не первый ордер, значит будем исполнять в случае если цена выгоднее предыдущей
                # и если мы не достигли максимального числа таких покупок
                if self.last_mark_price()*(1.0-self.new_trade_to_average_percent) > np.mean(self.executed_prices) and self.number_of_positions <= self.max_number_of_positions:
                    self.logger.info("Order number {}".format(self.number_of_positions))

                    if self.trade_market:
                        self.sell_market_with_leverage(self.tradable_qty, self.leverage)
                    else:
                        self.sell_with_leverage(float(self.get_last_close('1h')), self.tradable_qty, self.leverage)

                    qty, price = self.get_last_order_info()
                    self.logger.info("Executed qty {}, price {}".format(qty, price))

                    self.number_of_positions += 1
                    self.executed_prices.append(float(price))
                    print(self.executed_prices)
                    #self.executed_qts.append(float(qty))

        if decision == 1:
            self.logger.info("Buy market")
            # выполненых ордеров у нас нет, значит это должен быть первый
            if self.number_of_positions == 0:
                self.logger.info("First order")

                if self.trade_market:
                    self.buy_market_with_leverage(self.tradable_qty, self.leverage)
                else:
                    self.buy_with_leverage(float(self.get_last_close('1h')), self.tradable_qty, self.leverage)

                qty, price = self.get_last_order_info()
                self.logger.info("Executed qty {}, price {}".format(qty, price))

                self.number_of_positions = 1
                self.executed_prices.append(float(price))
                print(self.executed_prices)
                #self.executed_qts.append(float(qty))
            else:
                self.logger.info("Average buy price: {}".format(np.mean(self.executed_prices)))
                # это уже не первый ордер, значит будем исполнять в случае если цена выгоднее предыдущей
                # и если мы не достигли максимального числа таких покупок
                if self.last_mark_price()(1.0+self.new_trade_to_average_percent) < np.mean(self.executed_prices) and self.number_of_positions <= self.max_number_of_positions:
                    self.logger.info("Order number {}".format(self.number_of_positions))

                    if self.trade_market:
                        self.buy_market_with_leverage(self.tradable_qty, self.leverage)
                    else:
                        self.buy_with_leverage(float(self.get_last_close('1h')), self.tradable_qty, self.leverage)

                    qty, price = self.get_last_order_info()
                    self.logger.info("Executed qty {}, price {}".format(qty, price))

                    self.number_of_positions += 1
                    self.executed_prices.append(float(price))
                    print(self.executed_prices)
                    #self.executed_qts.append(float(qty))

        return
