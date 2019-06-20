# BitMEX trading platform.

### Overview
This is a trading bot for use with BitMEX. It is free to use and modify for your own strategies.

**Disclaimer: All persons who using this code do so at their own risk.**

**Develop on Testnet first!** \
Testnet trading is completely free and is identical to the live market.

---
### Getting started

```python
BotManager.addBot(account_name='bitmex.sample.trading', strategy=strategy_mas_extreme, trader=trader_mas_extreme,
                  strategy_params={'timeperiod': '30T'},
                  trader_params={'leverage': LEVERAGE, 'deposit_percent': PERCENT_MONEY_TO_TRADE, 'max_num_of_positions': AVERAGING,
                                 'new_trade_to_average_percent': DROPDOWN},
                  bitmex_params={'test': TEST_EXCHANGE,
                                 'api_key': API_KEY,
                                 'api_secret': API_SECRET})
```

Main information that one have to change is hidden in main_loop.py.
1. If you want to start from testnet.bitmex.com, then set **TEST_EXCHANGE** to True and put your credentials to 
**API_KEY** and **API_SECRET**. 
2. Then put percent of your deposit you want to trade in each trading execution (**PERCENT_MONEY_TO_TRADE**) 
and choose preferred leverage (**LEVERAGE**). Also you can set number of position for averaging (**AVERAGING**, e.g. 5) and 
percent of dropdown which will trigger averaging (**DROPDOWN**, e.g. 0.015).
3. Then set preferred timeframe for strategy (**TIMEFRAME**). Available variants are: 1m, 5m, 1h, 1d.
4. If you want change parameters of the strategy, go to strategies and choose prefered variant. For now, only 3 strategies are available.

---

### For advanced users

If you want to change work logic of bot, you have to change 2 main things:

1. Logic of prediction in Strategies (look at method **make_prediction**)
2. Logic of trade execution in Traders (look at method **exec_trade**)


---
#### Notes

Because I share my strategies, you can say thanks donating BTC right here:

**BTC**: 3BMEX2wvGPi85PStKLTZMC6E3nhdbyHEgA

Here my affiliate link for your registration on BitMEX.

https://www.bitmex.com/register/dduBF7

Users who have signed up with a valid affiliate link will receive a **10% fee discount for 6 months.**

#### Issues

Lots of people find this code not easy to run because of some dependencies. Here you can find libraries which are **essential** for succesful run:

1. talib (https://github.com/mrjbq7/ta-lib)
2. bitmex (https://github.com/BitMEX/api-connectors/tree/master/official-http/python-swaggerpy)
And finally, your python must be at least 3.6 version for using f-strings (ex: f"Last prediction: {prediction}")
Then you can successfully run the code.

If you have problems, feel free to open new issue.

