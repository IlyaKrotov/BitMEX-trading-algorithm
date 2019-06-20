from elasticsearch import Elasticsearch, helpers
import utils
import logging
from logging.handlers import RotatingFileHandler
import traceback
import pytz
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bitmex_websocket import BitMEXWebsocket
from threading import Thread, Lock

class BitmexTracker():

    index_prefix = 'btcusd.bitmex'

    def __init__(self, apikey=None, apisecret=None,
                 bitmex_address='wss://www.bitmex.com/realtime',
                 es_address='elasticsearch:9200'):

        self.running_lock = Lock()
        self.data_lock = Lock()
        self.reload_lock = Lock()

        self.apikey = apikey
        self.apisecret = apisecret
        self.bitmex_address = bitmex_address
        self.es_address = es_address

        self.written_trades_ids = {}
        self.current_tick = datetime.now()
        self.previous_tick = datetime.now()

        self.orderbook_width = 100

        self.setup_es()

        self.last_volume = 0
        self.volume_pool = []

        self.ticker = None
        self.asks = None
        self.bids = None
        self.trades = None
        self.instrument = None

        self.ws_logger = None
        self.es_logger = None

        self.es_thread = Thread(target=self.tick)
        self.es_thread.start()

    def tick(self):

        logger = self.setup_logger('ES')

        self.running_lock.acquire()

        while self.running_lock.locked():

            try:

                self.current_tick = datetime.now(pytz.utc).replace(microsecond=0)
                if (self.current_tick.second != self.previous_tick.second):

                    self.data_lock.acquire()
                    try:
                        if self.ticker:
                            ticker = self.ticker.copy()
                        else:
                            ticker = None

                        # if self.trades:
                        #     trades = self.trades.copy()
                        # else:
                        #     trades = None

                        if self.instrument:
                            instrument = self.instrument.copy()
                        else:
                            instrument = None

                        asks = self.asks
                        bids = self.bids
                    finally:
                        self.data_lock.release()  # release lock, no matter what

                    if asks is None or bids is None or ticker is None or instrument is None:
                        continue


                    logger.info('Ticker at timestamp.second {}: \033[94m{}\033[0m'
                                .format(self.current_tick.second, ticker))
                    ticker['timestamp'] = self.current_tick
                    self.es.create(index='{}.tickers'.format(self.index_prefix), id=utils.generate_nonce(),
                                   doc_type='ticker', body=ticker)

                    instrument['timestamp'] = self.current_tick
                    self.es.create(index='{}.instrument'.format(self.index_prefix), id=utils.generate_nonce(),
                                   doc_type='instrument', body=instrument)

                    # Calculate average volume
                    if instrument['volume'] != self.last_volume:
                        if instrument['volume'] < self.last_volume:
                            volume = instrument['volume']
                            self.last_volume = instrument['volume']
                        else:
                            volume = instrument['volume'] - self.last_volume
                        self.volume_pool.append(self.current_tick)
                        for timestamp in self.volume_pool:
                            self.es.create(index='{}.volumes'.format(self.index_prefix), id=utils.generate_nonce(),
                                          doc_type='volume', body={'timestamp': timestamp, 'volume': volume/len(self.volume_pool)})
                        self.volume_pool = []
                    else:
                        self.volume_pool.append(self.current_tick)
                    self.last_volume = instrument['volume']

                    for i in range(-1, -4, -1):

                        # последовательное уменьшение разрешения, L2->L1->L0
                        asks_frame = asks[-30*(-i):]
                        bids_frame = bids[:30*(-i)]

                        body = {
                            'timestamp': self.current_tick,
                            'bids': [tuple(x) for x in bids_frame.values],
                            'asks': [tuple(x) for x in asks_frame.values]
                        }
                        self.es.create(index='{}.orderbooks.l{}'.format(self.index_prefix, -i - 1),
                                       id=utils.generate_nonce(),
                                       doc_type='orderbook', body=body)
                        logger.info('Orderbook L{} at timestamp.second {}: ask \033[92m{}\033[0m, bid \033[91m{}\033[0m'
                                    .format(-i - 1, self.current_tick.second, body['asks'][-1], body['bids'][0]))

                        if self.current_tick.second % (10 ** (-i)) != 0:
                            break

                        bids['price'] = bids['price'].round(i)
                        asks['price'] = asks['price'].round(i)

                        asks = asks.groupby(['price']).agg({'size': np.sum}).reset_index()
                        bids = bids.groupby(['price']).agg({'size': np.sum}).reset_index()

                self.previous_tick = self.current_tick

            except:
                logger.error('\033[91m{}\033[0m'.format(traceback.format_exc()))
                try:
                    self.setup_es()
                except:
                    logger.error('Reloading ES')

    def get_ticker(self, logger):
        ticker = self.ws.get_ticker()
        self.data_lock.acquire()
        try:
            self.ticker = ticker
        finally:
            self.data_lock.release()

    def get_instrument(self, logger):
        instrument = self.ws.get_instrument()
        self.data_lock.acquire()
        try:
            self.instrument = instrument
        finally:
            self.data_lock.release()

    def get_orderbook(self, logger):

        while True:
            try:
                orderbook = self.ws.market_depth().copy()
                ob = pd.DataFrame(orderbook)[['price','side','size']]
                break
            except Exception as err:
                logger.error(str(err))
                ob = None

        if ob is not None:
            bids = ob[ob['side'] == 'Sell'][['price', 'size']].sort_values('price')
            asks = ob[ob['side'] == 'Buy'][['price', 'size']].sort_values('price')

            self.data_lock.acquire()
            try:
                self.bids = bids
                self.asks = asks
            finally:
                self.data_lock.release()  # release lock, no matter what

    def get_trades(self, logger):
        trades = self.ws.recent_trades()
        self.data_lock.acquire()
        try:
            self.trades = trades
        finally:
            self.data_lock.release()

                # self.es.create(index='btcusd.bitmex.trades', id=utils.generate_nonce(),
                #                doc_type='trades', body=trade)

    def get_funds(self, logger):
        funds = self.ws.funds()
        logger.info("Funds: %s" % funds)
        self.es.create(index='{}.funds'.format(self.index_prefix), id=utils.generate_nonce(),
                       doc_type='funds', body=funds)

    def setup_es(self):
        self.es = Elasticsearch(self.es_address)

        utils.create_index(self.es, '{}.tickers'.format(self.index_prefix))
        utils.create_index(self.es, '{}.orderbooks.l2'.format(self.index_prefix))
        utils.create_index(self.es, '{}.orderbooks.l1'.format(self.index_prefix))
        utils.create_index(self.es, '{}.orderbooks.l0'.format(self.index_prefix))
        utils.create_index(self.es, '{}.volumes'.format(self.index_prefix))
        utils.create_index(self.es, '{}.instrument'.format(self.index_prefix))
        utils.create_index(self.es, '{}.funds'.format(self.index_prefix))

    def run(self):

        logger = self.setup_logger('WS')

        try:
            # Instantiating the WS will make it connect. Be sure to add your api_key/api_secret.
            self.ws = BitMEXWebsocket(endpoint=self.bitmex_address, symbol="XBTUSD",
                                 api_key=None, api_secret=None)


            self.ws.get_instrument()

            logger.info('WS connector loaded')

            while self.ws.ws.sock.connected:
                self.get_ticker(logger)
                if self.ws.api_key:
                    self.get_funds(logger)
                self.get_orderbook(logger)
                self.get_instrument(logger)
                # self.get_trades(logger)
        except:
            logger.error('\033[91m{}\033[0m'.format(traceback.format_exc()))
        finally:
            self.running_lock.release()
            self.es_thread.join()
            logger.error('\033[91mTHIS IS THE END\033[0m')

    def setup_logger(self, name):
        # Prints logger info to terminal
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
        ch = RotatingFileHandler('collector.log', maxBytes=5000000, backupCount=5)
        # create formatter
        formatter = logging.Formatter("\033[93m%(asctime)s - %(name)s - %(levelname)s\033[0m - %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

if __name__ == "__main__":
    bt = BitmexTracker()
    bt.run()
