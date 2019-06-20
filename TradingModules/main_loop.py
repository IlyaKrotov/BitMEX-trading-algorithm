from Strategies.strategy_fast_rsi import strategy_fast_rsi
from Strategies.strategy_mas_extreme import strategy_mas_extreme
from Strategies.strategy_enhanced_fast import strategy_enhanced_fast

from Traders.trader_fast_rsi import trader_fast_rsi
from Traders.trader_mas_extreme import trader_mas_extreme
from Traders.trader_enhanced_fast import trader_enhanced_fast

from BotManager import BotManager

BotManager.addBot(account_name='', strategy=strategy_mas_extreme, trader=trader_mas_extreme,
                  strategy_params={'timeperiod': '30T'},
                  trader_params={'leverage': 10, 'deposit_percent': 0.01, 'max_num_of_positions': 7,
                                 'new_trade_to_average_percent': 0.015},
                  bitmex_params={'test': True,
                                 'api_key': "",
                                 'api_secret': ""})

BotManager.run()
