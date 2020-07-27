import os
import sys

parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)

from core import StrategyBase, ExecutionManager
from objects import (OrderbookData, TradeData, Instrument, TickerData, PriceData, IndexData, QuoteData, HoldAmountData, KLineData,
                     OrderUpdate, CancelAllOrdersResponse, CancelOrderResponse, PlaceOrderResponse,SummaryInfo)
from typedef import MarketDataType
import numpy as np
import pandas as pd
import pickle
import math
import traceback
from datetime import datetime
from utils import SABRLib,SABRUtil
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
import json
from scipy import interp
from websocket_server import WebsocketServer



 # Called for every client connecting (after handshake)
def _new_client(client, server):
     print(f"New client connected {client['address'][0]}" )
     '''
     clients[client['address'][0]] = {'default':True}
     if client['address'][0] not in clients:
         print(f"New client connected {client['address'][0]}" )
     '''
     pass
     

# Called for every client disconnecting
def _client_left(client, server):
    print(f"Client {client['address'][0]} disconnected")
    '''
    del clients[client['address'][0]]
    print(f"Client {client['address'][0]} disconnected")
    '''
    pass
    
# Called when a client sends a message
def _message_received(client, server, message):
    print(message)
    if message!=None:
        request = json.loads(message)
        data = getattr(strategy, request['func'])(request['args'])
        respond = {'func':f"on_{request['func']}", 'data':data}
        respond = str(json.dumps(respond))
        server.send_message(client,respond)

class OptionExecution(ExecutionManager):
    def __init__(self, strategy_name: str):
        super().__init__(strategy_name)
      
    def get_ticker(self, instruments:list, call_back_func):
        self.get_ticker_info(instruments,call_back_func)
        
        
class OptionStrategy(StrategyBase):
    def __init__(self, instruments: list, *args, **kwargs):
        #self.bidVol_SABR = None
        #self.askVol_SABR = None
        self.instruments = instruments
        self.cols = ['ask_price', 'bid_price', 'creation_timestamp','instrument_name', 
                     'interest_rate','open_interest','underlying_index','underlying_price', 'volume']

    def on_orderbook_data(self, data: OrderbookData):
        sym = data.symbol
        if data.bids.shape[0]>0:
            bid_price = data.bids[0,0]
        else:
            bid_price = -999999
        
        if data.asks.shape[0]>0:
            ask_price = data.asks[0,0]
        else:
            ask_price = -999999
        
        self.snapshot.set_value(sym, 'bid', bid_price)
        self.snapshot.set_value(sym, 'ask', ask_price)
        
        print(self.snapshot)

        
    
    def on_instrument_data(self, data: [Instrument]):
        BTC_options = [_ for _ in data if _.symbol.find('BTC')!=-1]
        print('done')

    def on_ticker_data(self, data):  
        print(data)
        
    def on_summaryinfo_data(self, data:pd.DataFrame): 
        currency = data.iloc[0]['base_currency']
        option_df=data[self.cols].dropna()
        option_list = [[sym.split('-')[1],sym.split('-')[2],sym.split('-')[3]] \
                for sym in option_df['instrument_name']]
        option_list = np.array(option_list)
        option_df['EXP_DATE'] = option_list[:,0]
        option_df['K'] = option_list[:,1].astype(float)
        option_df['cp'] = option_list[:,2]
        option_df['S'] = option_df['underlying_price']
        option_df['TTM'] = [1.0*(SABRUtil.time_tango(epd) - datetime.now()).days / 365 for epd in option_df['EXP_DATE']]
        vol_SABR=SABRLib.generateBV(option_df)
        #askVol_SABR=SABRLib.generateBV('Ask',option_df)
        fw = open(f'../data/{currency}_vol_SABR.pkl','wb')  
        pickle.dump(vol_SABR, fw)  
        fw.close() 
        vol_SABR['volDate'] = vol_SABR['volDate'].strftime("%Y%m%d")
        vol_SABR['currency'] = currency
        self.publish('Signal', 'SABR', vol_SABR)
        '''
        fw = open(f'../data/{currency}_askVol_SABR.pkl','wb')  
        pickle.dump(askVol_SABR, fw)  
        .close() 
         
        fw = open('../data/option_df.pkl','wb')  
        pickle.dump(option_df, fw)  
        fw.close() 
        ''' 
        
    def on_trade_data(self, data: TradeData):  
        print(f'On trade data. {data.instrument},Time:{data.local_time}, Price:{data.price}')
        
        
    def on_price_data(self, data: PriceData):
        pass

    def on_index_data(self, data: IndexData):
        pass

    def on_quote_data(self, data: QuoteData):
        pass

    def on_hold_amount_data(self, data: HoldAmountData):
        pass

    def on_kline_data(self, data: KLineData):
        pass

    def on_place_order_response(self, response: PlaceOrderResponse):
        pass

    def on_cancel_order_response(self, response: CancelOrderResponse):
        pass

    def on_cancel_all_orders_response(self, response: CancelAllOrdersResponse):
        pass

    def on_order_update(self, order_update: OrderUpdate):
        pass

    def thread(self):
        pass
    
    def register_user(self,args):
        client_ip = args
        clients[client_ip] = {'default':True}
        print(f"New client connected {client_ip}" )
    
    def generate_price(self, args):
        exp,stk,spread,currency,client_ip = args
        default = clients[client_ip]['default']
        exp = datetime.strptime("{}".format(exp+" 16:00:00"), "%Y-%m-%d %H:%M:%S")
        dtm = ((exp-datetime.now()).days+(exp-datetime.now()).seconds/3600/24)/365
        
        fw = open(f'../data/{currency}_vol_SABR.pkl','rb')  
        vol_SABR = pickle.load(fw)  
        fw.close() 
            
        if default:
            vol = SABRLib.get_vol(vol_SABR, dtm, float(stk))
        else:
            #client_ip = client['address'][0].replace(".","")
            fw = open(f'../data/Vol_SABR_{client_ip}.pkl','rb')  
            user_Vol_SABR = pickle.load(fw)  
            fw.close()
            vol = SABRLib.get_vol(user_Vol_SABR, dtm, float(stk))
            
        print(f'vol:{vol}, spread:{spread}')
        bid_vol = vol - float(spread)/100/2
        ask_vol = vol + float(spread)/100/2
        
        snapDateTime = datetime.now()
        #calculate the foward price by interpating expired date and future yield rate
        bitYield = vol_SABR['yieldCurve']['rate']
        ttm = vol_SABR['yieldCurve']['tenor']
        fwd = vol_SABR['fwdCurve']['rate'][0]
 
        fwdYield=interp(dtm,ttm, bitYield)
        fwd_price = fwd*math.exp(fwdYield*dtm)
        
        #calculate price base on exp,fwd and vol
        bid_call_option=SABRLib.BSmodel(float(stk), SABRUtil.py2ql_date(exp), 'C', 'forward')
        ask_call_option=SABRLib.BSmodel(float(stk), SABRUtil.py2ql_date(exp), 'C', 'forward')
        bid_put_option=SABRLib.BSmodel(float(stk), SABRUtil.py2ql_date(exp), 'P', 'forward')
        ask_put_option=SABRLib.BSmodel(float(stk), SABRUtil.py2ql_date(exp), 'P', 'forward')
        bid_call_option.price(SABRUtil.py2ql_date(snapDateTime), fwd_price, bid_vol, 0)
        ask_call_option.price(SABRUtil.py2ql_date(snapDateTime), fwd_price, ask_vol, 0)
        bid_put_option.price(SABRUtil.py2ql_date(snapDateTime), fwd_price, bid_vol, 0)
        ask_put_option.price(SABRUtil.py2ql_date(snapDateTime), fwd_price, ask_vol, 0)
        call_bid = bid_call_option.NPV()
        call_ask = ask_call_option.NPV()
        put_bid = bid_put_option.NPV()
        put_ask = ask_put_option.NPV()
        
        call_deltas = (bid_call_option.deltas() + ask_call_option.deltas())/2
        put_deltas = (bid_put_option.deltas() + ask_put_option.deltas())/2

        call_gamma = (bid_call_option.gamma() + ask_call_option.gamma())/2
        put_gamma = (bid_put_option.gamma() + ask_put_option.gamma())/2

        call_vega = (bid_call_option.vega() + ask_call_option.vega())/2
        put_vega = (bid_put_option.vega() + ask_put_option.vega())/2

        call_theta = (bid_call_option.theta() + ask_call_option.theta())/2
        put_theta = (bid_put_option.theta() + ask_put_option.theta())/2

        points = []
        point_id = 1
        if not default:
            vol_SABR = user_Vol_SABR
            #askVol_SABR = user_Vol_SABR
        for key, value in vol_SABR['volInfo'].items():
            #askValues = askVol_SABR['volInfo'][key]
            expiry = {}
            expiry['id'] = point_id
            point_id += 1
            expiry['exp'] =  key
            expiry['ATM'] =  value['ATMstrike']
            sub_points = []
            for i in range(len(value['optTicker'])):
                sub_point = {}
                sub_point['id'] = point_id
                sub_point['exp'] = key
                sub_point['symbol'] = value['optTicker'][i]
                sub_point['bid_iv'] = '%.2f' % value['bidVol'][i]
                sub_point['ask_iv'] = '%.2f' % value['askVol'][i]
                #sub_point['bid'] = '%.2f' % value['optPx'][i]
                #sub_point['ask'] = '%.2f' % askValues['optPx'][i]
                sub_points.append(sub_point)
                point_id +=1
            expiry['children'] = sub_points    
            points.append(expiry)    
 
        results = {
                   'call_bid':f"{'%.2f' % call_bid} ({'%.4f' % (call_bid/fwd)})",
                   'call_ask':f"{'%.2f' % call_ask} ({'%.4f' % (call_ask/fwd)})",
                   'put_bid':f"{'%.2f' % put_bid} ({'%.4f' % (put_bid/fwd)})",
                   'put_ask':f"{'%.2f' % put_ask} ({'%.4f' % (put_ask/fwd)})", 
                   'call_deltas':'%.2f' % call_deltas,
                   'put_deltas':'%.2f' % put_deltas,
                   'call_gamma':'%.6f' % call_gamma,
                   'put_gamma':'%.6f' % put_gamma,
                   'call_vega':'%.2f' % call_vega,
                   'put_vega':'%.2f' % put_vega,
                   'call_theta':'%.2f' % call_theta,
                   'put_theta':'%.2f' % put_theta,
                   #'put_bid_btc':'%.2f' % put_bid/fwd,
                   #'put_ask_btc':'%.2f' % put_ask/fwd,
                   'call_bid_iv':'%.2f' % bid_vol,
                   'call_ask_iv':'%.2f' % ask_vol,
                   'put_bid_iv':'%.2f' % bid_vol,
                   'put_ask_iv':'%.2f' % ask_vol,
                   'index_price':'%.2f' % fwd,
                   'fwd_price':'%.2f' % fwd_price,
                   'points':points
                }
        
        return results
    
    def user_gen_vols(self, args):
        points,currency,client_ip = args
        clients[client_ip]['default'] = False
        
        try:
            user_vol_SABR = SABRLib.customIV(points,currency)
            x = np.array([10/365,20/365,30/365,60/365,90/365,150/365,200/365])
            if currency=="BTC":
                y = np.array([i for i in range(1000,30250,250)])
            else:
                y = np.array([i for i in range(50,800,20)])
            X, Y = np.meshgrid(x, y)
            Z = SABRUtil.generateVols(x, y, user_vol_SABR)
            fig = plt.figure(figsize=(10,10))
            ax = plt.axes(projection='3d')
            ax.plot_surface(X, Y, Z, rstride=1, cstride=1,
                            cmap='viridis', edgecolor='none')
            ax.set_title(f'{currency} vol surface ('+str(user_vol_SABR['volDate'])+')')
            ax.set_xlabel('expiry (years)')
            ax.set_ylabel('strike ($)')
            ax.set_zlabel('Volatility (%)')
            
            ran_num =  datetime.now().microsecond
            file_name = client_ip+"_"+str(ran_num)+".png"
            fig.savefig(f"../web/images/{file_name}")
            fig.clear()
            
            print("before saving image")
            fw = open(f'../data/Vol_SABR_{client_ip}.pkl','wb')  
            print("after saving image 1 ")
            pickle.dump(user_vol_SABR, fw)
            print("after saving image 2")  
            fw.close()
            
        except:
            print(traceback.format_exc())
            return "error"
 
        return file_name
    
    
    def reset_vols(self, args):
        print(f'inside reset vol:{args}')
        currency,client_ip = args
        clients[client_ip]['default'] = True
        fw = open(f'../data/{currency}_vol_SABR.pkl','rb')  
        vol_SABR = pickle.load(fw)  
        fw.close() 
 
        x = np.array([10/365,20/365,30/365,60/365,90/365,150/365,200/365])
        if currency=="BTC":
            y = np.array([i for i in range(1000,30250,250)])
        else:
            y = np.array([i for i in range(50,800,20)])
        X, Y = np.meshgrid(x, y)
        Z = SABRUtil.generateVols(x, y, vol_SABR)
        fig = plt.figure(figsize=(10,10))
        ax = plt.axes(projection='3d')
        ax.plot_surface(X, Y, Z, rstride=1, cstride=1,
                        cmap='viridis', edgecolor='none')
        ax.set_title(f'{currency} vol surface ('+str(vol_SABR['volDate'])+')')
        ax.set_xlabel('expiry (years)')
        ax.set_ylabel('strike ($)')
        ax.set_zlabel('Volatility (%)')
        
        ran_num =  datetime.now().microsecond
        file_name = client_ip+"_"+str(ran_num)+".png"
        fig.savefig(f"../web/images/{file_name}")
        fig.clear()
        return file_name
        
        
           
    def heart_check(self,args):
        return True


if __name__ == '__main__':
    clients={}
    instruments = []
    btc_option = Instrument('Deribit', 'BTC', 'option', 'test_account',
                                    ['summaryinfo'])  
    eth_option = Instrument('Deribit', 'ETH', 'option', 'test_account',
                                    ['summaryinfo']) 
    instruments.append(btc_option)
    instruments.append(eth_option)
    
    strategy = OptionStrategy(instruments)
    strategy.start(instruments)
    
    server = WebsocketServer(8082,"0.0.0.0")
    server.set_fn_new_client(_new_client)
    server.set_fn_client_left(_client_left)
    server.set_fn_message_received(_message_received)
    server.run_forever()
    
