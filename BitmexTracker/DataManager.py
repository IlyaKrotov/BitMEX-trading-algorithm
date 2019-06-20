import os
import time
import logging
from logging.handlers import RotatingFileHandler
import shutil
import pandas as pd
import pytz
import requests
import elasticsearch
import progressbar
from functools import wraps
from datetime import datetime, timedelta

CONNECTION_TIMEOUT = 120
TIMES_TO_TRY = 3
RETRY_DELAY = 60

def generate_nonce():
    return int(time.time()*1000)

# Retry decorator for functions with exceptions
def retry(ExceptionToCheck, logger, tries=TIMES_TO_TRY, delay=RETRY_DELAY):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries = tries
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    print(e)
                    print('Retrying in %d seconds ...' % delay)
                    time.sleep(delay)
                    mtries -= 1
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck as e:
                print('Fatal Error: %s' % e)
                exit(1)

        return f_retry

    return deco_retry



class DataManager(object):

    logger = None

    class cache_subdirs():
        tickers = 'tickers'
        orderbooks = 'orderbooks'
        instruments = 'instruments'
        volumes = 'volumes'
        candles = 'candles'
        all_data = 'all_data'

    def __init__(self, host='91.235.136.166',
                 es_port=9200, influxdb_port=8086,
                 index_prefix='btcusd.bitmex',
                 cached_data_dir='./data', clean_cache=True,
                 *args, **kwargs):
        """

                :param host: ES host, if using ES locally change to localhost
                :param es_port: defaults to 9200
                :param influxdb_port: defaults to 8086
                :param index_prefix: defaults to 'btcusd.bitmex'
                :param cached_data_dir: path to cache files
                :param clean_cache: True or False, defaults to True
                :param test: defaults to real exchange
                :param **kwargs: bitmex apikey, apisecret e.t.c same as in bitmex connector
                """

        self.host = host
        self.es_port = es_port
        self.influxdb_port = influxdb_port

        self.index_prefix = index_prefix

        self.scroll_time = '30m'

        self.es = None
        self.influxdb = None
        self.create_es_connection()

        self.cached_data_dir = cached_data_dir

        if os.path.exists(self.cached_data_dir):
            if clean_cache:
                shutil.rmtree(self.cached_data_dir)
                os.makedirs(self.cached_data_dir)

        if DataManager.logger is None:
            DataManager.logger = logging.getLogger('DM')
            DataManager.logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
            ch = RotatingFileHandler('datamanager.log', maxBytes=5000000, backupCount=5)
            # create formatter
            formatter = logging.Formatter("\033[93m%(asctime)s - %(name)s - %(levelname)s\033[0m - %(message)s")
            # add formatter to ch
            ch.setFormatter(formatter)
            DataManager.logger.addHandler(ch)

    @retry(elasticsearch.exceptions.ConnectionError, logger, tries=TIMES_TO_TRY)
    def create_es_connection(self):
        """
                Connect to ES

                :return:
                """
        self.es = elasticsearch.Elasticsearch(self.host, timeout=CONNECTION_TIMEOUT)

    @retry(elasticsearch.exceptions.ConnectionError, logger, tries=TIMES_TO_TRY)
    def get_indices_by_re(self, re):
        """
                Get all ES indices matching with regular expression

                :param re: index to match
                :return:
                """
        indices = self.es.indices.get_alias(re)
        return indices

    @retry(elasticsearch.exceptions.ConnectionError, logger, tries=TIMES_TO_TRY)
    def unblock_index(self, index):
        """
                Force unblock write protected ES index

                :param index: index to unblock
                :return:
                """

        data = { "index": { "blocks": { "read_only_allow_delete": "false" } } }
        requests.put('{host}:{port}/{index}/_settings?pretty'.format(host=self.host, port=self.es_port, index=index), data)

    def download_data_by_time(self, index, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        if time_from is None:
            time_from = time_to - td

        page = self.es.search(
            index='{}.{}'.format(self.index_prefix, index),
            scroll=self.scroll_time,
            size=1000,
            body={ "query": { "range": { "timestamp": { "gte": time_from, "lt": time_to, } } } },
            sort='_id'
        )
        sid = page['_scroll_id']
        scrolled = len(page['hits']['hits'])
        scroll_size = page['hits']['total']

        if verbose:
            widgets = ['Downloading {index}'.format(index=str(index)),
                       progressbar.Bar(left='[', marker='#', right=']'),
                       progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
                       progressbar.Percentage(),
                       progressbar.FormatLabel('] [%(elapsed)s] ['),
                       progressbar.ETA(), '] [',
                       progressbar.FileTransferSpeed(unit='docs'), ']'
                       ]
            bar = progressbar.ProgressBar(
                widgets=widgets, maxval=scroll_size).start()
            bar.update(scrolled)

        data = [item['_source'] for item in page['hits']['hits']]
        df = pd.DataFrame(data).set_index('timestamp')

        while (scrolled < scroll_size):
            page = self.es.scroll(scroll_id=sid, scroll=self.scroll_time)
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scrolled += len(page['hits']['hits'])
            if verbose:
                bar.update(scrolled)
            data = [item['_source'] for item in page['hits']['hits']]
            df = df.append(pd.DataFrame(data).set_index('timestamp'))

        df.index = pd.to_datetime(df.index, format='%Y-%m-%d %H:%M:%S.%f')
        return df

    def __get_instruments_uncached(self, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        return self.download_data_by_time(index='instruments', td=td, time_from=time_from, time_to=time_to, verbose=verbose)

    def get_instruments(self, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        """
                Very different data, officially named 'instrument' in bitmex docs

                :param td: timedelta if not time_from
                :param time_from:
                :param time_to:
                :param verbose: dafaults to False
                :return:
                """
        return self.cacher(data_getter=self.__get_tickers_uncached, td=td, time_from=time_from, time_to=time_to, verbose=verbose, subdir=self.cache_subdirs.instruments)

    def __get_tickers_uncached(self, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        return self.download_data_by_time(index='tickers', td=td, time_from=time_from, time_to=time_to, verbose=verbose)

    def get_tickers(self, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        """
                Buy, sell, last, mid

                :param td: timedelta if not time_from
                :param time_from:
                :param time_to:
                :param verbose: dafaults to False
                :return:
                """
        return self.cacher(data_getter=self.__get_tickers_uncached, td=td, time_from=time_from, time_to=time_to, verbose=verbose, subdir=self.cache_subdirs.tickers)

    def __get_volumes_uncached(self, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        return self.download_data_by_time(index='volumes', td=td, time_from=time_from, time_to=time_to, verbose=verbose)

    def get_volumes(self, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        """
                Trade volumes

                :param td: timedelta if not time_from
                :param time_from:
                :param time_to:
                :param verbose: dafaults to False
                :return:
                """
        return self.cacher(data_getter=self.__get_volumes_uncached, td=td, time_from=time_from, time_to=time_to, verbose=verbose, subdir=self.cache_subdirs.volumes)

    def __get_orderbooks_uncached(self, level=0, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        return self.download_data_by_time(index='orderbooks.l{}'.format(level), td=td, time_from=time_from, time_to=time_to, verbose=verbose)

    def get_orderbooks(self, level=0, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        """
                Very different data, officially named 'instrument' in bitmex docs

                :param level: 0 - 0.5$ resolution, 1 - 10$, or 2 - 100$
                :param td: timedelta if not time_from
                :param time_from:
                :param time_to:
                :param verbose: dafaults to False
                :return:
                """
        return self.cacher(level=level, data_getter=self.__get_orderbooks_uncached, td=td, time_from=time_from, time_to=time_to, verbose=verbose, subdir=self.cache_subdirs.orderbooks)

    def __get_candles_uncached(self, period='1min', td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        tickers = self.cacher(td=td, time_from=time_from, time_to=time_to, verbose=verbose, data_getter=self.get_tickers, subdir=self.cache_subdirs.tickers)
        tickers.rename(columns = {'mid':'price'}, inplace = True)
        volumes = self.cacher(td=td, time_from=time_from, time_to=time_to, verbose=verbose, data_getter=self.get_volumes, subdir=self.cache_subdirs.volumes)
        df = pd.concat([tickers, volumes], axis=1)
        resampled_data = df.resample(period).agg({"price": "ohlc", "volume": "sum"})
        resampled_data.columns = resampled_data.columns.droplevel(0)
        return resampled_data

    def get_candles(self, period='1min', td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        """
                OHLCV

                :param period: e.g '1min' or '15min' or whatever appropriate to pandas.resample()
                :param td: timedelta if not time_from
                :param time_from:
                :param time_to:
                :param verbose: dafaults to False
                :return:
                """
        return self.cacher(period=period, data_getter=self.__get_candles_uncached, time_from=time_from, td=td, time_to=time_to, verbose=verbose, subdir=self.cache_subdirs.candles)

    def __get_TOV_uncached(self, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), include_instrument=False, verbose=False):
        orderbooks_l0 = self.cacher(td=td, time_from=time_from, time_to=time_to, verbose=verbose, data_getter=self.get_orderbooks, subdir=self.cache_subdirs.orderbooks)
        tickers = self.cacher(td=td, time_from=time_from, time_to=time_to, verbose=verbose, data_getter=self.get_tickers, subdir=self.cache_subdirs.tickers)
        volumes = self.cacher(td=td, time_from=time_from, time_to=time_to, verbose=verbose, data_getter=self.get_volumes, subdir=self.cache_subdirs.volumes)
        if include_instrument:
            instrument = self.cacher(td=td, time_from=time_from, time_to=time_to, verbose=verbose, data_getter=self.get_instruments, subdir=self.cache_subdirs.instruments)
            return pd.concat([orderbooks_l0, tickers, volumes, instrument], axis=1)
        return pd.concat([orderbooks_l0, tickers, volumes], axis=1)

    def get_TOV(self, td=timedelta(minutes=5), time_from=None, time_to=datetime.now(pytz.utc), verbose=False):
        """
                Tickers, Orderbooks, Volumes

                :param td: timedelta if not time_from
                :param time_from:
                :param time_to:
                :param verbose: dafaults to False
                :return: orderbooks l0, tickers and volumes in one dataframe
                """
        return self.cacher(data_getter=self.__get_TOV_uncached, time_from=time_from, td=td, time_to=time_to, verbose=verbose, subdir=self.cache_subdirs.all_data)

    def create_index(self, index):
        if (self.es.indices.exists(index)):
            logging.info('Already Exists, Skipping:! ' + index)
            return True
        else:
            self.es.indices.create(index)
        return False

    def __format_datetime(self, dt):
        return dt.strftime('%Y_%m_%d_%H')

    def __check_folder(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def cacher(self, subdir, data_getter, time_from, td, time_to, verbose, *args, **kwargs):

        cache_dir = self.__check_folder(self.cached_data_dir + '/' + self.index_prefix.replace('.', '_'))
        cache_dir = self.__check_folder(cache_dir + '/' + subdir)

        if time_from is None:
            time_from = time_to - td

        time_ = time_from.replace(minute=0, second=0, microsecond=0)
        batches_ready = 0

        df = pd.DataFrame()

        if verbose:
            widgets = ['Getting {index} data by one-hour batches'.format(index=str(subdir)),
                       progressbar.Bar(left='[', marker='#', right=']'),
                       progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
                       progressbar.Percentage(),
                       progressbar.FormatLabel('] [%(elapsed)s] ['),
                       progressbar.ETA(), '] [',
                       progressbar.FileTransferSpeed(unit='batches'), ']'
                       ]
            bar = progressbar.ProgressBar(
                widgets=widgets, maxval=time_to.hour - time_from.hour).start()
            bar.update(batches_ready)

        time_to_compare = time_to.replace(minute=0, second=0, microsecond=0)

        while time_ < time_to_compare:
            filename = cache_dir + '/' + '{time_from}_{time_to}'.format(
                time_from=self.__format_datetime(time_),
                time_to=self.__format_datetime(time_ + timedelta(hours=1))
            ) + '_'.join(['_%s=%s' % (key, value) for (key, value) in kwargs.items()]) + '.csv'
            try:
                df_h = pd.read_csv(filename)
                if 'timestamp' in df_h:
                    df_h = df_h.set_index('timestamp')
            except FileNotFoundError:
                df_h = data_getter(**kwargs, time_from=time_, time_to=time_ + timedelta(hours=1))
                df_h.to_csv(filename)
            df_h.index = pd.to_datetime(df_h.index, format='%Y-%m-%d %H:%M:%S.%f')
            if time_ == time_from.replace(minute=0, second=0, microsecond=0): df_h = df_h[df_h.index >= time_from]
            df = df.append(df_h)
            if verbose:
                batches_ready += 1
                bar.update(batches_ready)
            time_ = time_ + timedelta(hours=1)

        if time_ < time_to:
            df_h = data_getter(**kwargs, time_from=time_to.replace(minute=0, second=0, microsecond=0), time_to=time_to)
            if 'timestamp' in df_h:
                df_h = df_h.set_index('timestamp')
            df = df.append(df_h)

        if verbose:
            batches_ready += 1
            bar.update(time_to.hour - time_from.hour)

        return df

    def write_data(self, index, data, doc_type='doc', time=datetime.now(pytz.utc)):
        data['timestamp'] = time
        self.es.create(index=index, id=generate_nonce(), doc_type=doc_type, body=data)
