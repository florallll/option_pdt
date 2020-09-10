from functools import reduce
from threading import Thread
import pandas as pd
from abc import ABCMeta, abstractmethod
from utils.intercom import Intercom
from typedef import (MarketDataType, IntercomScope,
                     IntercomChannel, ActionType)
from objects import (CancelAllOrdersResponse, CancelOrderResponse, InspectOrderResponse, QueryHistoryOrdersResponse,
                     QueryHistoryTradesResponse, QueryOrdersResponse, PlaceOrderResponse, HoldAmountData, IndexData,
                     KLineData, OrderbookData, PriceData, QuoteData,Instrument,SummaryInfo, TradeData, OrderUpdate,TickerData)

_MARKET_DATA_PARSER = {
    MarketDataType.Trade: TradeData,
    MarketDataType.Orderbook: OrderbookData,
    MarketDataType.KLine: KLineData,
    MarketDataType.HoldAmount: HoldAmountData,
    MarketDataType.Index: IndexData,
    MarketDataType.Price: PriceData,
    MarketDataType.Quote: QuoteData,
    MarketDataType.QuoteTicker: TickerData,
    MarketDataType.Instrument: Instrument,
    MarketDataType.SummaryInfo: SummaryInfo
}

_RESPONSE_DATA_PARSER = {
    ActionType.PlaceOrder: PlaceOrderResponse,
    ActionType.CancelOrder: CancelOrderResponse,
    ActionType.CancelAllOrders: CancelAllOrdersResponse,
    ActionType.InspectOrder: InspectOrderResponse,
    ActionType.QueryOrders: QueryOrdersResponse,
    ActionType.QueryHistoryOrders: QueryHistoryOrdersResponse,
    ActionType.QueryHistoryTrades: QueryHistoryTradesResponse
}

class StrategyBase(metaclass=ABCMeta):
    @property
    def strategy_name(self) -> str:
        return type(self).__name__

    @abstractmethod
    def on_trade_data(self, data: TradeData):
        raise NotImplementedError()

    @abstractmethod
    def on_orderbook_data(self, data: OrderbookData):
        raise NotImplementedError()
        
    @abstractmethod
    def on_price_data(self, data: PriceData):
        raise NotImplementedError()

    @abstractmethod
    def on_index_data(self, data: IndexData):
        raise NotImplementedError()

    @abstractmethod
    def on_quote_data(self, data: QuoteData):
        raise NotImplementedError()
    
    @abstractmethod
    def on_summaryinfo_data(self, data:pd.DataFrame):
        raise NotImplementedError()
        
    @abstractmethod
    def on_instrument_data(self, data: [Instrument]):
        raise NotImplementedError()
        
    @abstractmethod
    def on_hold_amount_data(self, data: HoldAmountData):
        raise NotImplementedError()

    @abstractmethod
    def on_kline_data(self, data: KLineData):
        raise NotImplementedError()

    @abstractmethod
    def on_place_order_response(self, response: PlaceOrderResponse):
        raise NotImplementedError()

    @abstractmethod
    def on_cancel_order_response(self, response: CancelOrderResponse):
        raise NotImplementedError()

    @abstractmethod
    def on_cancel_all_orders_response(self, response: CancelAllOrdersResponse):
        raise NotImplementedError()

    @abstractmethod
    def on_order_update(self, order_update: OrderUpdate):
        raise NotImplementedError()

    @abstractmethod
    def thread(self):
        raise NotImplementedError()

    @classmethod
    def publish(self, scope, channel, data):
        self.engine._intercom.publish(scope, channel, data)
        
    @classmethod
    def add_subscription(self, subscriptions):
        self.engine._add_subscription(subscriptions)

    @classmethod
    def start(self, instruments: list,*args, **kwargs):
        self.engine = StrategyEngine(self, instruments)
        self.engine.start(*args, **kwargs)


class StrategyEngine(object):
    def __init__(self, strategy_class, instruments: list,*args, **kwargs):
        assert issubclass(strategy_class, StrategyBase), TypeError(
            f'strategy class must be subclass of StrategyBase')
        self._strategy = strategy_class(instruments,*args, **kwargs)
        self._instruments = instruments
        self._intercom = Intercom()

    def _on_market_data(self, message: dict):
        data_type = message['data_type']
        data = _MARKET_DATA_PARSER[data_type].from_json(message)
        getattr(self._strategy, f'on_{data_type}_data')(data)

    def _on_response(self, message: dict):
        action_type = message['action_type']
        data = _RESPONSE_DATA_PARSER[action_type].from_json(message)
        getattr(self._strategy, f'on_{action_type}_response')(data)

    def _on_order_update(self, message: dict):
        obj = OrderUpdate.from_json(message)
        self._strategy.on_order_update(obj)

    def _on_position_info(self, data):
        pass

    def _initialize_intercom(self):
        subscriptions = reduce(
            list.__add__, [_.subscriptions for _ in self._instruments])
        accounts = [_.account_name for _ in self._instruments]
        #发送market相关请求
        self._intercom.publish(IntercomScope.Market,
                               IntercomChannel.SubscribeRequest, subscriptions)
        print(IntercomScope.Market,IntercomChannel.SubscribeRequest, subscriptions)
        #订阅market相关推送
        for channel in subscriptions:
            self._intercom.subscribe(
                IntercomScope.Market, channel, self._on_market_data)
            print(IntercomScope.Market, channel, self._on_market_data)
        #查询account相关订单     
        self._intercom.publish(IntercomScope.Trade,
                               IntercomChannel.SubscribeOrderUpdate, accounts)
        #查询account相关持仓    
        self._intercom.publish(
            IntercomScope.Position, IntercomChannel.PollPositionInfoRequest, accounts)
         #订阅account相关持仓和订单信息    
        for account in accounts:
            self._intercom.subscribe(
                IntercomScope.Trade, account, self._on_order_update)
            self._intercom.subscribe(
                IntercomScope.Position, account, self._on_position_info)
        #订阅策略交易信息  
        self._intercom.subscribe(
            IntercomScope.Trade, f'{self._strategy.strategy_name}_response', self._on_response)
        
    def _add_subscription(self,subscriptions):
        print('itsssss here')
        self._intercom.publish(IntercomScope.Market,IntercomChannel.SubscribeRequest, subscriptions)
        print(IntercomScope.Market,IntercomChannel.SubscribeRequest, subscriptions)
        #订阅market相关推送
        for channel in subscriptions:
            self._intercom.subscribe(IntercomScope.Market, channel, self._on_market_data)
            print(IntercomScope.Market, channel, self._on_market_data)
        

    def start(self):
        self._initialize_intercom()
        Thread(target=self._intercom.thread).start()
        Thread(target=self._strategy.thread).start()
