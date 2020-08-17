from base.strategy_base import StrategyBase
from configs.typedef import TradeSide, OrderType, Direction,OrderStatus
import numpy as np
import pandas as pd
from datetime import datetime

class Template(StrategyBase):
    def __init__(self):
        super().__init__()
        self.subscribe_allow = True
        self.sent_order = False
        self.order_existed = False #control late inplace response
        self.auto_hedge = True
        self.account_equity = 0
        self.BTCUSD_option_value = 0
        self.hedge_positions = {}
        self.local_hedge_info = {}
        self.empty_pos = {'qty':0,'side':0}
        self.orders = {}
        self.symbol_dict = {}
        self.sent_orderId = None
        self.sides=np.array([TradeSide.Buy,TradeSide.Sell])
        self.position_del_cols = ['account_id','result','timestamp']
        self.position_cols = ['qty','side']
        self.order_cols = ['symbol','contract_type','direction','avg_executed_price','quantity','filled_quantity','status','created_time']
        self.quote_cols = ['last_price','volume','best_bid','best_bid_amount','best_ask','best_ask_amount','mark_price','mark_iv','ask_iv','bid_iv','gamma','theta','delta','rho','vega','timestamp']

    def config_update(self, **config):
        super().config_update(**config)
        self.symbol_number = len(config['subscription_list'])
        self.qtys = np.random.randint(1,10,self.symbol_number)/10
        side_index = np.random.randint(0,2,self.symbol_number)
        self.trade_sides = self.sides[side_index]
        #self.instrument = 'Deribit|BTCUSD|perp'

    #If spread is wider than one ticksize(0.5), add one tick to the best price for the maker order
    #For taker order, use the best price as the limit price
    def send_limit_order(self,market_data,delta_diff,note):
        qty = int(abs(delta_diff) * market_data['last_price']/10)
        if qty<=0:
            return
        order_price = 0
        spread = market_data['best_ask'] - market_data['best_bid']
        if note['aggresive'] == "maker":
            if delta_diff > 0:
                side = Direction.Buy
                if spread > 0.5:
                    order_price = market_data['best_bid'] + 0.5
                    print("maker, best bid plus one tick")
                else:
                    order_price = market_data['best_bid']
                    print("maker, best bid")
            elif delta_diff < 0:
                side = Direction.Sell
                if spread > 0.5:
                    order_price = market_data['best_ask'] - 0.5
                    print("maker, best ask minus one tick")
                else:
                    order_price = market_data['best_ask']
                    print("maker, best ask")
        else:
            if delta_diff > 0:
                side = Direction.Buy
                order_price = market_data['best_ask']
                print("Taker, best_ask")
            elif delta_diff < 0:
                side = Direction.Sell
                order_price = market_data['best_bid']
                print("Taker, best_bid")

        if order_price!=0:
            self.send_order('Deribit|BTCUSD|perp', side, order_price, qty, OrderType.Limit,note=note)
            self.order_existed = True
