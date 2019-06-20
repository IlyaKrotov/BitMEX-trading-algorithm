import time

from Functions.data_preparation import write_state
from Functions.logger import create_logger
from datetime import datetime
import pytz

from influxdb import InfluxDBClient
from Functions.consts import influxdb_conndata

from Functions.data_preparation import get_last_candles

class BotManager():

    bots = {}
    influx = None

    def __init__(self, account_name, strategy, trader, bitmex_params, trader_params, strategy_params, restore_state=True):
        self.name = account_name + '_' + strategy.__name__ + '_' +  trader.__name__
        self.account_name = account_name
        self.strategy_name = strategy.__name__
        self.trader_name = trader.__name__
        self.logger = create_logger(self.name)
        self.strategy = strategy(self.logger, **strategy_params)
        self.trader = trader(self.logger, self.strategy, bitmex_params, **trader_params)
        if restore_state:
            self.trader.load_state(self.account_name)
        self.initial_balance, _ = self.trader.balances_status()

    def log_balance(self):
        total, realised_pnl = self.trader.balances_status()

        # for plots
        write_state('../logs/{}/balances.csv'.format(self.name), total)
        write_state('../logs/{}/realised_pnls.csv'.format(self.name), realised_pnl)

        status = [
            {
                "measurement": "balance",
                "tags": {
                    "account": self.account_name,
                    "strategy": self.strategy_name,
                    "trader": self.trader_name,
                },
                "time": datetime.now(pytz.utc),
                "fields": {
                    'Total balance': total,
                    'Balance change since start': ((total + realised_pnl) / self.initial_balance - 1.) * 100,
                    'Realised PnL': realised_pnl
                }
            }
        ]
        BotManager.influx.write_points(status)

        self.logger.info(
            f"Total balance: {total}, "
            f"Balance change since start: {((total+realised_pnl)/self.initial_balance - 1.)*100}%, "
            f"Realised PnL: {realised_pnl}")

        time_last_check = time.time()

    @staticmethod
    def addBot(account_name, strategy, trader, bitmex_params, trader_params, strategy_params, restore_state=True):
        bot = BotManager(account_name, strategy, trader, bitmex_params, trader_params, strategy_params, restore_state)
        BotManager.bots[bot.name] = bot

    @staticmethod
    def run():

        BotManager.influx = InfluxDBClient(**influxdb_conndata)

        logger = create_logger('bot_manager')
        logger.info("Start of work")

        start_time_offset = 0.5
        time_to_wait_new_trade = 60# * 60
        time_to_wait_new_balance_check = 61# * 10

        safe_stop = False

        while not safe_stop:
            try:
                now = round(time.time())

                if now % time_to_wait_new_trade == 0:
                    logger.info("Start make prediction")
                    time.sleep(start_time_offset)
                    dt_trade = time.time()
                    candles = get_last_candles(timeframe='1m')

                    for bot in BotManager.bots:
                        BotManager.bots[bot].trader.exec_trade(candles)
                        BotManager.bots[bot].trader.save_state(BotManager.bots[bot].account_name)
#                        time.sleep(0.5)

                    dt_trade = time.time() - dt_trade
                    time_last_trade = time.time() - dt_trade

                if now % time_to_wait_new_balance_check == 0:

                    for bot in BotManager.bots:
                        BotManager.bots[bot].log_balance()

            except Exception as e:
                logger.info("EXCEPTION: " + str(e))
            except KeyboardInterrupt:
                safe_stop = True
                logger.info('Stopping gracefully')
            finally:
                time.sleep(1)

