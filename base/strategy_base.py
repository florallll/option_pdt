import json
import logging
import time
from threading import Thread, Lock
from queue import PriorityQueue

from utils.intercom import Intercom
from utils.util_func import get_timestamp
from configs import EXCHANGE_CONFIGS
from configs.typedef import OrderActions, IntercomScope, IntercomChannel, RequestActions, Event


class StrategyBase:
    def __init__(self):
        self.intercom = Intercom()
        self.trading = False
        self.ref_id = 0
        self.lock = Lock()
        self.logger = logging.getLogger()
        self.handlers = {}
        self.periodic_funcs = PriorityQueue()
        self.strategy_config = {
            'strategy_key': None,
            'strategy_name': None,
            'subscription_list': [],
            'signal_names': [],
            'account_ids': {}
        }

    def config_update(self, **config):  # 更新策略config
        """
        更新策略基本信息
        :strategy_name: 策略名称
        :strategy_key: 策略key
        :signal_names: （可选）订阅的信号名称
        :subscribe_channels: （可选）订阅的数据channel
        :account_ids: 交易账号，格式为{exchange: account_id}
        """
        for key in self.strategy_config.keys():
            self.strategy_config[key] = config.get(key, self.strategy_config[key])
        self.logger.info(f'StrategyBase::config updated: strategy_config={self.strategy_config}')

    def register_handler(self, scope, channel, handler):  # 注册redis channel对应回调
        key = f'{scope}:{channel}'
        if key in self.handlers:
            self.logger.warning(f'StrategyBase::a handler has already been registered to this key: {key}')
        self.handlers[key] = handler

    def set_interval(self, sec, func):  # 新增一个周期任务，func会每隔n秒调用一次
        self.logger.info(f'StrategyBase::add periodic task. function={func}, interval={sec}seconds')
        self.periodic_funcs.put((get_timestamp(), sec, func))

    def _register_handlers(self):
        self.register_handler(IntercomScope.Trade, f'{self.strategy_config["strategy_name"]}_response',
                              self.on_response_ready_handler)
        #注册用户position booking事件
        self.register_handler(IntercomScope.OptionPosition, IntercomChannel.PositionUpdate,self.on_position_booking_handler)
        #注册用户auot hedge change事件
        self.register_handler(IntercomScope.OptionPosition, IntercomChannel.AutoHedgeUpdate,self.on_auto_hedge_handler)
         #注册用户account target change事件
        self.register_handler(IntercomScope.OptionPosition, IntercomChannel.TargetUpdate,self.on_target_update_handler)
        #注册exchange position update事件
        for exchange, account_id in self.strategy_config['account_ids'].items():
            self.register_handler(IntercomScope.Position, f'{exchange}|{account_id}', self.on_position_ready_handler)
            #self.register_handler(IntercomScope.Trade, f'{exchange}|{account_id}', self.on_order_ready_handler)
        #注册市场数据update事件
        for channel in self.strategy_config['subscription_list']:
            self.register_handler(IntercomScope.Market, channel, self.on_market_data_ready_handler)
        #for signal_name in self.strategy_config['signal_names']:
            #self.register_handler(IntercomScope.Signal, signal_name, self.on_signal_ready_handler)
        #self.register_handler(IntercomScope.Signal, Event.OnSignalManagerStart, self.on_signal_manager_starts_handler)
        self._subscribe_data_feed()

    def _subscribe_data_feed(self):  # 订阅策略的市场数据和信号
        self.logger.info(f'StrategyBase::subscribe data feed')
        '''
        request = {
            'channels': self.strategy_config['subscription_list'],
            'signal_names': self.strategy_config['signal_names']
        }
        self.intercom.emit(IntercomScope.Signal, IntercomChannel.SubscribeRequest, request)
        '''
        print(f"sending {self.strategy_config['subscription_list']}")
        if len(self.strategy_config['subscription_list'])>0:
            self.intercom.emit(IntercomScope.Market, IntercomChannel.SubscribeRequest, self.strategy_config['subscription_list'])

    def _subscribe_position_info(self):  # 订阅策略的仓位信息
        self.logger.info(f'StrategyBase::subscribe position info')
        for exchange, account_id in self.strategy_config['account_ids'].items():
            self.intercom.emit(IntercomScope.Position, IntercomChannel.PollPositionInfoRequest,
                               f'{exchange}|{account_id}')

    def _subscribe_order_update(self):  # 订阅策略的订单回报信息
        self.logger.info(f'StrategyBase::subscribe order update')
        for exchange, account_id in self.strategy_config['account_ids'].items():
            self.intercom.emit(IntercomScope.Trade, IntercomChannel.ORDER_UPDATE_SUBSCRIPTION_REQUEST,
                               f'{exchange}|{account_id}')

    def _publish_request(self, action, metadata):
        metadata['account_id'] = self.strategy_config['account_ids'][metadata['exchange']]
        request = {
            'strategy': self.strategy_config['strategy_name'],
            'ref_id': f'{self.strategy_config["strategy_key"]}_{self.ref_id}',
            'action': action,
            'metadata': metadata
        }
        self.ref_id += 1
        logging.debug(f"StrategyBase::publish request: action={action}, metadata={metadata}")
        self.intercom.emit(IntercomScope.Trade, f'{self.strategy_config["strategy_name"]}_request', request)

    def on_market_data_ready_handler(self, data):
        data_type = data['data_type']
        handler = getattr(self, f'on_market_data_{data_type}_ready')
        if handler:
            handler(data)

    def on_position_ready_handler(self, data):
        self.on_position_ready(data)

    def on_position_booking_handler(self, data):
        self.on_position_booking(data)
    
    def on_auto_hedge_handler(self, data):
        self.on_auto_hedge(data)

    def on_target_update_handler(self,data):
        self.on_target_update(data)

    def on_order_ready_handler(self, data):
        self.on_order_ready(data)

    def on_response_ready_handler(self, data):
        if data['ref_id'].startswith(self.strategy_config['strategy_key']):
            action = data['action']
            handler = getattr(self, f'on_response_{action}_ready')
            if handler:
                handler(data)

    def on_signal_ready_handler(self, data):
        self.on_signal_ready(data)

    def on_signal_manager_starts_handler(self, data):
        self.logger.info(f'StrategyBase::On signal manager starts.')
        self._subscribe_data_feed()

    def main_thread(self):  # 主线程，从redis接收数据并推送至各个回调函数
        self.pubsub = self.intercom.subscribe(self.handlers.keys())
        for message in self.pubsub.listen():
            key = message['channel'].decode('utf8')
            data = json.loads(bytes.decode(message['data']))
            self.handlers[key](data)

    def _add_subscritions(self,scope,subscription_list):
        channels = []
        for subscription in subscription_list:
            key = f'{scope}:{subscription}'
            self.handlers[key] = self.on_market_data_ready_handler
            channels.append(key)
        self.pubsub.subscribe(channels)
        self.intercom.emit(IntercomScope.Market, IntercomChannel.SubscribeRequest, subscription_list)

    
    def periodic_thread(self):  # 周期线程
        while True:
            while self.periodic_funcs.queue[0][0] < get_timestamp():
                _, interval, func = self.periodic_funcs.get()
                func()
                self.periodic_funcs.put((get_timestamp() + interval, interval, func))
            time.sleep(1)

    def sabr_thread(self):
        channel = f'{IntercomScope.Signal}:SABR'
        self.handlers[channel] = self.on_signal_ready_handler
        self.intercom.emit(IntercomScope.Signal, 'SABR', '','sabr_host')
        sabr_pubsub = self.intercom.subscribe(channel,'sabr_host')
        for message in sabr_pubsub.listen():
            key = message['channel'].decode('utf8')
            data = json.loads(bytes.decode(message['data']))
            self.handlers[key](data)

    def start(self):
        self.trading = True
        self._register_handlers()
        self._subscribe_position_info()
        #self._subscribe_order_update()
        Thread(target=self.main_thread).start()
        Thread(target=self.periodic_thread).start()
        Thread(target=self.sabr_thread).start()

    # For templates
    def send_order(self, instrument, direction, price, quantity, order_type, **kwargs):
        exchange, symbol, contract_type = instrument.split('|')
        if contract_type!="option":
            instrument_id = ""
            quantity = round(quantity, EXCHANGE_CONFIGS[exchange]['SIZE_DECIMAL'][f'{symbol}|{contract_type}'])
        else:
            instrument_id = symbol
            symbol = symbol.split('-')[0]
            quantity = round(quantity, EXCHANGE_CONFIGS[exchange]['SIZE_DECIMAL'][f'{symbol}|{contract_type}'])
        metadata = {
            'exchange': exchange,
            'symbol': symbol,
            'instrument_id':instrument_id,
            'contract_type': contract_type,
            'direction': direction,
            'price': price,
            'quantity': quantity,
            'order_type': order_type
        }
        metadata.update(kwargs)
        self._publish_request(OrderActions.Send, metadata)

    def cancel_order(self, instrument, order_id, **kwargs):
        exchange, symbol, contract_type = instrument.split('|')
        instrument_id = ""
        if contract_type=="option":
            instrument_id = symbol
            symbol = symbol.split('-')[0]
        metadata = {
            'exchange': exchange,
            'symbol': symbol,
            'instrument_id':instrument_id,
            'contract_type': contract_type,
            'order_id': order_id
        }
        metadata.update(kwargs)
        self._publish_request(OrderActions.Cancel, metadata)

    def cancel_all_orders(self, instrument):
        exchange, symbol, contract_type = instrument.split('|')
        metadata = {
            'exchange': exchange,
            'symbol': symbol,
            'contract_type': contract_type
        }
        self._publish_request(OrderActions.CancelAll, metadata)

    def modify_order(self, instrument, order_id, price, quantity, **kwargs):
        exchange, symbol, contract_type = instrument.split('|')
        if contract_type!="option":
            instrument_id = ""
            #quantity = round(quantity, EXCHANGE_CONFIGS[exchange]['SIZE_DECIMAL'][f'{symbol}|{contract_type}'])
        else:
            instrument_id = symbol
            symbol = symbol.split('-')[0]
            #quantity = round(quantity, EXCHANGE_CONFIGS[exchange]['SIZE_DECIMAL'][f'{symbol}|{contract_type}'])
        metadata = {
            'exchange': exchange,
            'symbol': symbol,
            'instrument_id':instrument_id,
            'contract_type': contract_type,
            'price': price,
            'quantity': quantity,
            'order_id': order_id
        }
        metadata.update(kwargs)
        self._publish_request(OrderActions.Modify, metadata)

    def inspect_order(self, instrument, order_id , **kwargs):
        exchange, symbol, contract_type = instrument.split('|')
        instrument_id = ""
        if contract_type=="option":
            instrument_id = symbol
            symbol = symbol.split('-')[0]
        metadata = {
            'exchange': exchange,
            'symbol': symbol,
            'instrument_id':instrument_id,
            'contract_type': contract_type,
            'order_id': order_id
        }
        metadata.update(kwargs)
        self._publish_request(OrderActions.Inspect, metadata)

    def query_orders(self, instrument):
        exchange, symbol, contract_type = instrument.split('|')
        metadata = {
            'exchange': exchange,
            'symbol': symbol,
            'contract_type': contract_type
        }
        self._publish_request(RequestActions.QueryOrders, metadata)

    def get_order_history(self, instrument, size=100):
        exchange, symbol, contract_type = instrument.split('|')
        metadata = {
            'account_id': self.strategy_config['account_ids']['exchange'],
            'exchange': exchange,
            'symbol': symbol,
            'contract_type': contract_type,
            'size': size
        }
        self._publish_request(RequestActions.QueryHistoryOrders, metadata)

    def query_balance(self, exchange, currency, account_type='spot'):
        metadata = {
            'account_id': self.strategy_config['account_ids']['exchange'],
            'exchange': exchange,
            'currency': currency,
            'account_type': account_type
        }
        self._publish_request(RequestActions.QueryBalance, metadata)

    
    def send_data_to_user(self, data):
        self.intercom.emit(IntercomScope.Strategy,IntercomChannel.StrategyResponse,data)

    # For strategies.

    def pre_start(self, **kwargs):
        self.logger.warning("Strategy base has no implementation for pre_start")

    def on_market_data_orderbook_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_orderbook_ready")

    def on_market_data_trade_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_trade_ready")
    
    def on_market_data_summaryinfo_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_summaryinfo_ready")
    
    def on_market_data_ticker_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_ticker_ready")

    def on_market_data_quote_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_quote_ready")

    def on_market_data_funding_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_funding_ready")

    def on_market_data_index_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_index_ready")

    def on_market_data_kline_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_kline_ready")

    def on_market_data_quote_ticker_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_quote_ticker_ready")

    def on_market_data_bar_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_market_data_bar_ready")

    def on_response_place_order_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_response_place_order_ready")

    def on_response_cancel_order_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_response_cancel_ready")

    def on_response_modify_order_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_response_modify_order_ready")

    def on_response_inspect_order_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_response_inspect_order_ready")

    def on_response_cancel_all_orders_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_response_cancel_all_orders_ready")

    def on_response_query_balance_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_response_query_balance_ready")

    def on_response_query_position_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_response_query_position_ready")

    def on_position_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_position_ready")

    def on_position_booking(self, data):
        self.logger.warning("Strategy base has no implementation for on_position_booking")

    def on_order_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_order_ready")

    def on_signal_ready(self, data):
        self.logger.warning("Strategy base has no implementation for on_signal_ready")

    def on_auto_hedge(self,data):
        self.logger.warning("Strategy base has no implementation for on_auto_hedge")

    def on_target_update(self,data):
        self.logger.warning("Strategy base has no implementation for on_target_update")
