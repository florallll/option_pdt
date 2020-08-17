import time
import json
import logging
import numpy as np
import pandas as pd
from copy import deepcopy
from threading import Thread
from utils.util_func import get_human_readable_timestamp, get_timestamp
from configs.typedef import IntercomScope, IntercomChannel, MarketData, TradeSide, Event
from utils.intercom import Intercom
from library import get_signal

_PROCESSES = 8


class Aggregator:
    def __init__(self, channel):
        self.channel = channel
        self.exchange, self.symbol, self.contract_type, self.data_type = channel.split('|')
        self.subscription_list = ['|'.join([self.exchange, self.symbol, self.contract_type, MarketData.Orderbook]),
                                  '|'.join([self.exchange, self.symbol, self.contract_type, MarketData.Trade])]
        self.timedelta = pd.Timedelta(self.data_type).total_seconds()
        self.timestamp = get_timestamp()
        self.metadata = {
            'open': float('nan'),
            'high': float('nan'),
            'low': float('nan'),
            'close': float('nan'),
            'volume': 0,
            'buy_volume': 0,
            'sell_volume': 0,
            'asks': [],
            'bids': [],
            'mid': float('nan')
        }

    def reset_metadata(self):
        self.timestamp = get_timestamp()
        for _ in ['open', 'high', 'low', 'close']:
            self.metadata[_] = self.metadata['close']
        for _ in ['volume', 'buy_volume', 'sell_volume']:
            self.metadata[_] = 0

    def on_market_data_ready_handler(self, data):
        data_type = data['data_type']
        if data_type == MarketData.Trade:
            self.on_market_data_trade_ready(data)
        else:
            self.on_market_data_orderbook_ready(data)

    def on_market_data_orderbook_ready(self, data):
        self.metadata['asks'] = data['metadata']['asks']
        self.metadata['bids'] = data['metadata']['bids']
        self.metadata['mid'] = (self.metadata['asks'][0][0] + self.metadata['bids'][0][0]) / 2

    def on_market_data_trade_ready(self, data):
        for trade in data['metadata']:
            price = trade[2]
            side = trade[3]
            size = trade[-1]
            if np.isnan(self.metadata['open']):
                self.metadata['open'] = price
                self.metadata['high'] = price
                self.metadata['low'] = price
            else:
                self.metadata['high'] = max(self.metadata['high'], price)
                self.metadata['low'] = min(self.metadata['low'], price)
            self.metadata['close'] = price
            self.metadata['volume'] += size
            if side == TradeSide.Buy:
                self.metadata['buy_volume'] += size
            else:
                self.metadata['sell_volume'] += size


class SignalManager:
    def __init__(self):
        self.intercom = Intercom()
        self.subscriptions = {
            'channels': [],
            'signal_names': []
        }
        self.signals = {}
        self.aggregators = {}
        self.subscription_list = []
        self.handlers = {}

    def _subscribe_data_feed(self):
        if self.subscription_list:
            logging.debug(f'SignalManager::subscribe data feed. channels={self.subscription_list}')
            self.intercom.emit(IntercomScope.Market, IntercomChannel.SubscribeRequest, self.subscription_list)

    def register_handler(self, channel, handler):
        if channel not in self.handlers:
            self.handlers[channel] = []
        if handler not in self.handlers[channel]:
            self.handlers[channel].append(handler)

    def configs_update(self):
        channels = self.subscriptions['channels'].copy()
        self.signals = {_: self.signals.get(_, get_signal(_)) for _ in self.subscriptions['signal_names']}
        for signal in self.signals.values():
            channels += signal.subscription_list
            for channel in signal.subscription_list:
                self.register_handler(channel, signal.on_market_data_ready_handler)
        subscription_list = [_ for _ in channels if _.split('|')[3][0].isalpha()]
        aggregate_channels = [_ for _ in channels if _.split('|')[3][0].isdigit()]
        self.aggregators = {_: self.aggregators.get(_, Aggregator(_)) for _ in aggregate_channels}
        for aggregator in self.aggregators.values():
            subscription_list += aggregator.subscription_list
            for channel in aggregator.subscription_list:
                self.register_handler(channel, aggregator.on_market_data_ready_handler)
        self.subscription_list = list(set(subscription_list))
        logging.debug(f'SignalManager::subscription update. {self.subscriptions}')
        self._subscribe_data_feed()

    def on_market_data_ready_handler(self, channel, data):
        handlers = self.handlers.get(channel, [])
        for handler in handlers:
            response = handler(data)
            if response is not None:
                self.intercom.emit(IntercomScope.Signal, response['signal_name'], response)

    def on_subscribe_handler(self, data):
        channels = data.get('channels', [])
        signal_names = data.get('signal_names', [])
        logging.debug(f'SignalManager::On subscribe. channels={channels}, signal_names={signal_names}')
        self.subscriptions['channels'] += [_ for _ in channels if _ not in self.subscriptions['channels']]
        self.subscriptions['signal_names'] += [_ for _ in signal_names if _ not in self.subscriptions['signal_names']]
        self.configs_update()

    def on_unsubscribe_handler(self, data):
        pass

    def on_feed_manager_starts_handler(self):
        logging.debug(f'SignalManager::On feed manager starts.')
        self._subscribe_data_feed()

    def main_thread(self):
        pubsub = self.intercom.subscribe([f'{IntercomScope.Signal}:{IntercomChannel.SubscribeRequest}',
                                          f'{IntercomScope.Signal}:{IntercomChannel.UnsubscribeRequest}',
                                          f'{IntercomScope.Market}*'])
        for message in pubsub.listen():
            scope, channel = message['channel'].decode('utf8').split(':')
            data = json.loads(bytes.decode(message['data']))
            if scope == IntercomScope.Market:
                if channel == Event.OnFeedManagerStart:
                    self.on_feed_manager_starts_handler()
                elif channel in self.handlers.keys():
                    self.on_market_data_ready_handler(channel, data)
                else:
                    pass
            else:
                if channel == IntercomChannel.SubscribeRequest:
                    self.on_subscribe_handler(data)
                else:
                    self.on_unsubscribe_handler(data)

    def periodic_thread(self):
        while True:
            for agg in self.aggregators.values():
                if get_timestamp() - agg.timestamp > agg.timedelta:
                    response = {
                        'exchange': agg.exchange,
                        'symbol': agg.symbol,
                        'contract_type': agg.contract_type,
                        'data_type': agg.data_type,
                        'timestamp': get_human_readable_timestamp(),
                        'metadata': deepcopy(agg.metadata)
                    }
                    self.intercom.emit(IntercomScope.Market, agg.channel, response)
                    agg.reset_metadata()
            time.sleep(1)

    def start(self):
        logging.debug('SignalManager::Start')
        self.intercom.emit(IntercomScope.Signal, Event.OnSignalManagerStart, Event.OnSignalManagerStart)
        self.configs_update()
        self._subscribe_data_feed()
        Thread(target=self.main_thread).start()
        Thread(target=self.periodic_thread).start()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    mgr = SignalManager()
    mgr.start()
