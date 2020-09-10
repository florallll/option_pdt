import os
import sys

parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)

from core import StrategyBase, ExecutionManager
from objects import (OrderbookData, TradeData, Instrument, TickerData, PriceData, IndexData, QuoteData, HoldAmountData, KLineData,
                     OrderUpdate, CancelAllOrdersResponse, CancelOrderResponse, PlaceOrderResponse,SummaryInfo,TickerData)
from typedef import MarketDataType, IntercomScope, IntercomChannel
import numpy as np
import pandas as pd
import pickle
import time
import math
import traceback
from datetime import datetime
from utils.AmberOptions import *
from utils import SABRUtil
import utils.SABRLib_old as SABRLib
# import utils.SABRLib_copy as SABRLib
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
import json
from numpy import interp
from pathos.multiprocessing import ProcessPool
from websocket_server import WebsocketServer


def chart_greeks(args):
    params,key = args
    Type = params['Type']
    S = float(params['S'])
    X = float(params['X'])
    upper = max(X, float(params['U']))
    lower = max(X, float(params['L']))
    s_max,s_min = max(S, upper),min(S, lower)
    
    s_range=np.arange(s_min*0.5,s_max*1.5,S*0.05,dtype=float)
    
    s_l = [str(round(w,2)) for w in s_range]
    t_range = np.arange(1,365,1)
    t_l = [str(w) for w in t_range]
    #display list needs to be string
    
    S_call_data = Options(params,'c').chart_greeks(s_range,'S')
    S_put_data = Options(params,'p').chart_greeks(s_range,'S')
    T_call_data = Options(params,'c').chart_greeks(t_range/365,'T')
    T_put_data = Options(params,'p').chart_greeks(t_range/365,'T')

    label = ['premium','delta','gamma','vega','theta']
    labelx = ['x','premium','delta','gamma','vega','theta']
    
    S_call_data = pd.DataFrame(S_call_data,columns=label)
    S_put_data = pd.DataFrame(S_put_data,columns=label)
    T_call_data = pd.DataFrame(T_call_data,columns=label)
    T_put_data = pd.DataFrame(T_put_data,columns=label)
    
    S_call_data['x'],S_put_data['x'] = s_l,s_l
    T_call_data['x'],T_put_data['x'] = t_l,t_l
    
    data={
        'key':key,
        "chartData_sc_display":{'columns':labelx,'rows':S_call_data.to_dict(orient = 'records')},
        "chartData_sp_display":{'columns':labelx,'rows':S_put_data.to_dict(orient = 'records')},
        "chartData_tc_display":{'columns':labelx,'rows':T_call_data.to_dict(orient = 'records')},
        "chartData_tp_display":{'columns':labelx,'rows':T_put_data.to_dict(orient = 'records')}
    }

    return data

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
    print('message',message)
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
        self.option_df = pd.DataFrame([])
        self.instruments = instruments
        self.subscriptions = []
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
        print('inside ticker',data)
        
        # print('option_df',self.option_df.columns)
        
        # try:
        #     if data['instrument_id'] in list(self.option_df['instrument_name'] ):
                
        #         print(self.option_df[self.option_df['instrument_name'] == data['instrument_id']])
                
        # except:
        #     pass
        
    def on_summaryinfo_data(self, data:pd.DataFrame): 
        # print('in summary info',data.keys())

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
        # print(SABRUtil.time_tango(i) for i in  option_df['EXP_DATE'])
        self.option_df = option_df
        vol_SABR=SABRLib.generateBV(option_df)
        # print(vol_SABR)
        #askVol_SABR=SABRLib.generateBV('Ask',option_df)
        fw = open(f'../data/{currency}_vol_SABR.pkl','wb')  
        pickle.dump(vol_SABR, fw)  
        fw.close() 
        vol_SABR['volDate'] = vol_SABR['volDate'].strftime("%Y%m%d")
        vol_SABR['currency'] = currency
        # self.publish('Signal', 'SABR', vol_SABR)
        s_l = ['Deribit|BTCUSD-20200925-10000-P|option|ticker']
        if s_l not in self.subscriptions:
            self.subscriptions.append(s_l)
            self.add_subscription(s_l)
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

    def chart_payoff(self,args):
        payoff_list = {}
        dic,S,spot_qty = args
        ccr = list(dic.values())[0]["params"]["ccr"]
        S = float(S)
        s_range = np.arange(S*0.3, S*1.7, S*0.0025)
        payoff_list["s"] = [str(int(w)) for w in s_range]

        option_payoff = s_range * 0.0
        basket_bid,basket_ask,deltas,gammas,vegas,thetas, n_id = 0.,0.,0.,0.,0.,0.,0  # duplicate option posiiton identfier

        for key, value in dic.items():
            qty = float(value['qty'])
            if ccr != value["params"]["ccr"]:
                return 414
            data = {
                "s": s_range,
                "params": value["params"],
                "premium": {
                    "c": {'bid':  float(value["premium"][0].split(" (")[0]),
                          'ask':  float(value["premium"][1].split(" (")[0]),
                    },
                    "p": {'bid': float(value["premium"][2].split(" (")[0]),
                          'ask':float(value["premium"][3].split(" (")[0]),
                    }
                },
                "deltas": { "c": float(value["deltas"][0]), "p": float(value["deltas"][1])},
                "gammas": { "c": float(value["gammas"][0]), "p": float(value["gammas"][1])},
                "vegas":  { "c": float(value["vegas"][0]),"p": float(value["vegas"][1])},
                "thetas": { "c": float(value["thetas"][0]),"p": float(value["thetas"][1])},
            }
            for i in value["position"]:
                data["position"] = i["value"]
                data["position_display"] = i["label"]
                payoff_data = Payoff(data)
                delta,gamma,vega,theta = payoff_data.count_greeks()
                deltas += delta*qty
                gammas += gamma*qty
                vegas += vega*qty
                thetas += theta*qty
                payoff_result = payoff_data.result()*qty
                bid,ask = payoff_data.price()
                basket_bid += bid*qty
                basket_ask += ask*qty
                _id = payoff_data.id + "-" + str(n_id)
                payoff_list[_id] = payoff_result
                option_payoff += payoff_result
            n_id += 1

        payoff_list["Option_Payoff"] = option_payoff
        payoff_label = ["s","Option_Payoff"]

        if float(spot_qty) != 0.:
          spot_payoff = payoff_data.spot(spot_qty) 
          total_payoff = spot_payoff + option_payoff
          payoff_list['Spot'] = spot_payoff
          payoff_list['Total_Payoff'] = total_payoff
          payoff_label.append('Total_Payoff')

        payoff_df = pd.DataFrame(payoff_list)
        total_payoff_df = payoff_df.filter(payoff_label, axis=1)
        
        payoff_dict={
            "individual_payoff": {'columns':list(payoff_list.keys()),'rows':payoff_df.to_dict(orient='records')},
            'total_payoff': {'columns':payoff_label,'rows':total_payoff_df.to_dict(orient='records')},
            'deltas': round(deltas,3),'gammas': round(gammas,4),'vegas': round(vegas,3),'thetas': round(thetas,3),
            'payoff_spot' : ccr,'basket_price':{'bid': basket_bid , 'ask': basket_ask }
        }
    
        return payoff_dict

    def chart_greeks_start(self, args):
        self.pool = ProcessPool()
        res = self.pool.apipe(chart_greeks, (args))
        return res.get()

    def generate_price(self, args):
        Type = args['Type']
        exp = args['exp']
        stk = float(args['X'])
        spread = args['spread']
        currency = args['ccr']
        client_ip = args['client_ip']
        params = args
      
        default = clients[client_ip]['default']
        exp = datetime.strptime("{}".format(exp+" 16:00:00"), "%Y-%m-%d %H:%M:%S")
        dtm = ((exp-datetime.now()).days+(exp-datetime.now()).seconds/3600/24)/365
        #fw = open(f'/home/amber/option_pricing/api/data/{currency}_vol_SABR.pkl','rb')  
        
        fw = open(f'../data/{currency}_vol_SABR.pkl','rb')  
        vol_SABR = pickle.load(fw)  
        fw.close() 
        
        if default:
            if Type!= 'Binary-double-no-touch':
                vol = SABRLib.get_vol(vol_SABR, dtm, float(stk))
            else:
                L,U = float(params['L']),float(params['U'])
                lu=np.arange(L,U,5)
                v=[SABRLib.get_vol(vol_SABR, dtm, _ ) for _ in lu]
                vol_l,vol_u = min(v),max(v)
        else:
            #client_ip = client['address'][0].replace(".","")
            fw = open(f'../data/Vol_SABR_{client_ip}.pkl','rb')  
            user_Vol_SABR = pickle.load(fw)  
            fw.close()
            if Type!= 'Binary-double-no-touch':
                vol = SABRLib.get_vol(user_Vol_SABR, dtm, float(stk))
            else:
                L,U = float(params['L']),float(params['U'])
                lu = np.arange(L,U,5)
                v=[SABRLib.get_vol(user_Vol_SABR, dtm, _ ) for _ in lu]
                vol_l,vol_u = min(v),max(v)

        snapDateTime = datetime.now()
        #calculate the foward price by interpating expired date and future yield rate
        bitYield = vol_SABR['yieldCurve']['rate']
        ttm = vol_SABR['yieldCurve']['tenor']
        fwd = vol_SABR['fwdCurve']['rate'][0]
 
        fwdYield = interp(dtm,ttm, bitYield)
        fwd_price = fwd*math.exp(fwdYield*dtm)
    
        params['S'],params['T'],params['r']=fwd_price,dtm,fwdYield

        #calculate price base on exp,fwd and vol
        if  Type=='Barrier':
            if (args['barrier_flag']=='uo')& (fwd_price > float(args['H'])):
                return 404
            if (args['barrier_flag']=='do')&(fwd_price < float(args['H'])):
                return 401
        elif Type=='Double-barrier':
            if (args['outin_flag']=='o')&(fwd_price<float(args['L']))|(fwd_price>float(args['U'])):
                return 402  

        call_option_bid = Options(params.copy(),'c')
        put_option_bid  = Options(params.copy(),'p')
        call_option_ask = Options(params.copy(),'c')
        put_option_ask  = Options(params.copy(),'p')
        
        if Type!='Binary-double-no-touch':
            print(f"vol:{vol}, spread:{spread}")
            bid_vol = vol - float(spread) / 100 / 2
            ask_vol = vol + float(spread) / 100 / 2
            call_option_bid.setattr('v',bid_vol)
            put_option_bid.setattr('v',bid_vol)
            call_option_ask.setattr('v',ask_vol)
            put_option_ask.setattr('v',ask_vol)
            call_bid_iv,put_bid_iv,call_ask_iv,put_ask_iv = bid_vol,bid_vol,ask_vol,ask_vol
            params['v'] = vol
        else:
            print(f"vol:{vol_l,vol_u}, spread:{spread}")
            bid_vol_l = vol_l - float(spread) / 100 / 2
            bid_vol_u = vol_u - float(spread) / 100 / 2
            ask_vol_l = vol_l + float(spread) / 100 / 2
            ask_vol_u = vol_u + float(spread) / 100 / 2

            call_option_bid.setattr('vol_l',bid_vol_l)
            put_option_bid.setattr('vol_u',bid_vol_u)
            call_option_ask.setattr('vol_l',ask_vol_l)
            put_option_ask.setattr('vol_u',ask_vol_u)
            params['vol_l'],params['vol_u'] = vol_l,vol_u
            call_bid_iv,call_ask_iv,put_bid_iv,put_ask_iv = bid_vol_l,ask_vol_l,bid_vol_u,ask_vol_u

        call_bid,call_delta_bid,call_gamma_bid,call_vega_bid,call_theta_bid = call_option_bid.greeks()
        put_bid,put_delta_bid,put_gamma_bid,put_vega_bid,put_theta_bid = put_option_bid.greeks()
        call_ask,call_delta_ask,call_gamma_ask,call_vega_ask,call_theta_ask = call_option_ask.greeks()
        put_ask,put_delta_ask,put_gamma_ask,put_vega_ask,put_theta_ask = put_option_ask.greeks()
    
        call_deltas = (call_delta_ask+call_delta_bid)/2
        put_deltas = (put_delta_ask+put_delta_bid)/2

        call_gamma = (call_gamma_ask+call_gamma_bid)/2
        put_gamma = (put_gamma_ask+put_gamma_bid)/2

        call_vega = (call_vega_ask+call_vega_bid)/2
        put_vega = (put_vega_ask+put_vega_bid)/2

        call_theta = (call_theta_ask+call_theta_bid)/2
        put_theta = (put_theta_ask+put_theta_bid)/2

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
                sub_points.append(sub_point)
                point_id +=1
            expiry['children'] = sub_points    
            points.append(expiry)    
  
       
        if call_vega < 0:
            call_ask,call_bid = call_bid,call_ask
            call_bid_iv,call_ask_iv = call_ask_iv,call_bid_iv
        if put_vega < 0 :
            put_bid,put_ask = put_ask,put_bid
            put_bid_iv,put_ask_iv = put_ask_iv,put_bid_iv

        results = {
                   'call_bid':f"{'%.2f' % call_bid} ({'%.4f' % (call_bid/fwd)}) ({'%.2f' % (((call_bid/fwd)/(1+(call_bid/fwd))/dtm)*100)} %)",
                   'call_ask':f"{'%.2f' % call_ask} ({'%.4f' % (call_ask/fwd)}) ({'%.2f' % (((call_ask/fwd)/(1+(call_ask/fwd))/dtm)*100)} %)",
                   'put_bid':f"{'%.2f' % put_bid} ({'%.4f' % (put_bid/fwd)}) ({'%.2f' % (((put_bid/fwd)/(1+(put_bid/fwd))/dtm)*100)} %)",
                   'put_ask':f"{'%.2f' % put_ask} ({'%.4f' % (put_ask/fwd)}) ({'%.2f' % (((put_ask/fwd)/(1+(put_ask/fwd))/dtm)*100)} %)", 
                   'call_deltas':'%.2f' % call_deltas,
                   'put_deltas':'%.2f' % put_deltas,
                   'call_gamma':'%.6f' % call_gamma,
                   'put_gamma':'%.6f' % put_gamma,
                   'call_vega':'%.3f' % call_vega,
                   'put_vega':'%.3f' % put_vega,
                   'call_theta':'%.3f' % call_theta,
                   'put_theta':'%.3f' % put_theta,
                   'call_bid_iv':'%.2f' % call_bid_iv,
                   'call_ask_iv':'%.2f' % call_ask_iv,
                   'put_bid_iv':'%.2f' % put_bid_iv,
                   'put_ask_iv':'%.2f' % put_ask_iv,
                   'index_price':'%.2f' % fwd,
                   'fwd_price':'%.2f' % fwd_price,
                   'points':points,
                   'params':params,
                   'stamp':int(time.time())
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
                ax.plot_surface(X, Y, Z, rstride=1, cstride=1,cmap='viridis', edgecolor='none')
                ax.set_title(f'{currency} vol surface ('+str(user_vol_SABR['volDate'])+')')
                ax.set_xlabel('expiry (years)')
                ax.set_ylabel('strike ($)')
                ax.set_zlabel('Volatility (%)')
                
                ran_num =  datetime.now().microsecond
                file_name = client_ip+"_"+str(ran_num)+".png"
                fig.savefig(f"../web/images/{file_name}")
                fig.clear()
                
                fw = open(f'../data/Vol_SABR_{client_ip}.pkl','wb')  
                pickle.dump(user_vol_SABR, fw)  
                fw.close()
            except:
                print(traceback.format_exc())
                return "error"
    
            return file_name
   
    def reset_vols(self, args):
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
            ax.plot_surface(X, Y, Z, rstride=1, cstride=1,cmap='viridis', edgecolor='none')
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
    btc_option = Instrument('Deribit', 'BTC', 'option', 'test_account',['summaryinfo'])  
    eth_option = Instrument('Deribit', 'ETH', 'option', 'test_account',['summaryinfo']) 
    options = Instrument('Deribit', 'BTCUSD-20200925-10000-C', 'option', 'test_account',['ticker']) 

    instruments.append(btc_option)
    instruments.append(options)
    
    strategy = OptionStrategy(instruments)
    strategy.start(instruments)

    server = WebsocketServer(8081,"0.0.0.0")
    server.set_fn_new_client(_new_client)
    server.set_fn_client_left(_client_left)
    server.set_fn_message_received(_message_received)
    server.run_forever()
    
