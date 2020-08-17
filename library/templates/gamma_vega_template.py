from base.strategy_base import StrategyBase
from configs.typedef import TradeSide, OrderType, Direction,OrderStatus
from utils.util_func import getATMstk
import numpy as np
import pandas as pd
import pickle
import traceback
from datetime import datetime

class Template(StrategyBase):
    def __init__(self):
        super().__init__()
        self.basket_update = False
        self.auto_hedge = True
        self.orders_existed = {} #control late inplace response
        self.orders = {}
        self.symbol_dict = {}
        self.local_hedge_info = {}
        self.sent_orders_dict = {}
        self.new_syms = {} #the new orders to be sent
        self.cancel_syms = {} #the orders to be cancelled
        self.baskets = []
        self.empty_pos = {'qty':0,'side':0}
        self.position_del_cols = ['account_id','result','timestamp']
        self.order_cols = ['symbol','contract_type','direction','avg_executed_price','quantity','filled_quantity','status','created_time']
        self.quote_cols = ['last_price','volume','best_bid','best_bid_amount','best_ask','best_ask_amount','mark_price','mark_iv','ask_iv','bid_iv','gamma','theta','delta','rho','vega','timestamp']

    def config_update(self, **config):
        super().config_update(**config)
        self.subscription_list = config['subscription_list']

        
    #If spread is wider than one ticksize, add one tick to the best price for the maker order
    #For taker order, use the best price as the limit price
    def send_limit_order(self,symbol,market_data,note):
        instrument = f'Deribit|{symbol}|option'
        diff = market_data['diff']
        qty = abs(diff)
        tick_size = 0.0005
        #print(f'send limit order qty :{qty}')
        if qty==0 or qty > 20:
            return
        order_price = 0
        spread = market_data['best_ask'] - market_data['best_bid']
        if note['aggresive'] == "maker":
            if diff > 0:
                side = Direction.Buy
                if spread > tick_size:
                    order_price = market_data['best_bid'] + tick_size
                    print("maker, best bid plus one tick")
                else:
                    order_price = market_data['best_bid']
                    print("maker, best bid")
            elif diff < 0:
                side = Direction.Sell
                if spread > tick_size:
                    order_price = market_data['best_ask'] - tick_size
                    print("maker, best ask minus one tick")
                else:
                    order_price = market_data['best_ask']
                    print("maker, best ask")
        else:
            if diff > 0:
                side = Direction.Buy
                order_price = market_data['best_ask']
                print("Taker, best_ask")
            elif diff < 0:
                side = Direction.Sell
                order_price = market_data['best_bid']
                print("Taker, best_bid")
        
        print(f'order_price:{order_price},instrument:{instrument}')
        if order_price!=0:
            self.send_order(instrument, side, order_price, qty, OrderType.Limit,note=note)
            self.orders_existed[symbol] = True

    def generate_basket(self,positions):
        baskets = []
        instrument_list = []
        if len(positions) > 0:
            try:
                with open(f'data/option_df.pkl','rb') as fw:
                    optData = pickle.load(fw)  
                #groups = list(set(sym.split('-')[1] for sym in positions.index))      
                groups = list(set(positions['group']))
                for exp_date in groups:
                    target = {'group':exp_date}
                    optDataSub = optData[(optData['EXP_DATE']==exp_date) & (3*optData['bid_price'] > optData['ask_price'])]  
                    optDataSub = optDataSub.sort_values('volume',ascending=False)
                    target['syms'] = optDataSub[0:self.contract_num]['instrument_id'].values
                    baskets.append(target)
                    instrument_list.extend(target['syms'])
            except:
                self.logger.error(traceback.format_exc())

        return baskets,instrument_list

    '''
    def generate_basket(self,positions,hedge_positions):
        baskets = []
        instrument_list = []
        if len(positions) > 0: 
            with open(f'data/option_df.pkl','rb') as fw:
                optData = pickle.load(fw)  
            #groups = list(set(sym.split('-')[1] for sym in positions.index))      
            groups = list(set(positions['group']))
            for exp_date in groups:
                target = {'group':exp_date}
                optDataSub = optData[(optData['EXP_DATE']==exp_date) & (3*optData['bid_price'] > optData['ask_price'])]  
                optDataSub = optDataSub.sort_values('volume',ascending=False)
                target_contracts = optDataSub[0:2]['instrument_id'].values
                target['gamma'] = target_contracts[0]
                target['vega'] = target_contracts[1]
                baskets.append(target)
                instrument_list.extend(target_contracts)
        return baskets,instrument_list
    
    def generate_basket(self,positions,hedge_positions):
        baskets = []
        instrument_list = []
        with open(f'data/option_df.pkl','rb') as fw:
            optData = pickle.load(fw)  
        #groups = list(set(sym.split('-')[1] for sym in positions.index))      
        groups = list(set(positions['group']))
        for exp_date in groups:
            target = {'group':exp_date,'clear_list':[]}
            for key,value in hedge_positions.items():
                instrument = key.split('_')[0]
                currency,expiry,strike,cp = instrument.split('-')
                if expiry == exp_date:
                   
                    exp = datetime.strptime("{}".format(expiry+" 16:00:00"), "%Y%m%d %H:%M:%S")
                    dtm = ((exp-datetime.now()).days+(exp-datetime.now()).seconds/3600/24)
                    if dtm > 3:
                        if not 'gamma' in target:
                            target['gamma'] =  instrument
                        else:
                            target['vega'] =  instrument

            for greek in ['gamma','vega']:
                if greek not in target:
                    contract = self.select_contract(optData,exp_date,greek,target)
                    target[greek]=contract
                instrument_list.append(target[greek])
            baskets.append(target)
                
        return baskets,instrument_list
    
    def select_contract(self,optData,exp_date,greek,target):
        optDataSub = optData[(optData['EXP_DATE']==exp_date) & (3*optData['bid_price'] > optData['ask_price'])]  
        optDataSub = optDataSub.sort_values('volume',ascending=False)
        target_contracts = optDataSub[0:2]['instrument_id'].values
        
        if greek == 'gamma':
            if 'vega' in target:
                if target['vega']!=target_contracts[0]:
                    return target_contracts[0]
                else:
                    return target_contracts[1]
            else:
                return target_contracts[0]
        else:
            if 'gamma' in target:
                if target['gamma']!=target_contracts[0]:
                    return target_contracts[0]
                else:
                    return target_contracts[1]
            else:
                return target_contracts[0]
                
    '''


   
        
            
        
