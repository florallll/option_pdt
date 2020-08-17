# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 15:51:05 2019

@author: justin.chen
"""
import os
import sys

parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)

import json
import pickle
import traceback
from threading import Thread
import pandas as pd
from websocket_server import WebsocketServer
from utils.intercom import Intercom
from configs.typedef import IntercomScope,IntercomChannel
from utils.util_func import getATMstk,remove_expired_positions,NpEncoder
from datetime import datetime

class BookingService:
    def __init__(self, intercom):       
        self.intercom = intercom
        with open(f'{data_path}/customer_position.pkl','rb') as fw:
            self.positions = pickle.load(fw)  
        self.cols = ['symbol','avg_price','qty','side','close_price','group','customer','settlement_ccy']

    def book_position(self,client_id,args):
        with open(f'{data_path}/option_df.pkl','rb') as fw:
            optData = pickle.load(fw)  
        #options' expire dates are longer than 3 days
        optData = optData[optData['TTM'] > 3]
        optExp = pd.Series(optData['EXP_DATE'].values.ravel()).unique()
        exp_list = [datetime.strptime("{}".format(d), '%Y%m%d') for d in optExp]
        exp,strike,price,currency,cp,side,customer,qty,settlement_ccy,request_id = args
        side = 1 if side == "Buy" else -1
        exp_date = datetime.strptime("{}".format(exp), '%Y-%m-%d')
        group_date = getATMstk(exp_date,exp_list)
        group_date = group_date.strftime("%Y%m%d")
        currency = currency+"USD"
        exp_date = exp_date.strftime("%Y%m%d")
        instrument = '-'.join([currency,exp_date,str(strike),cp])
        row_data = [instrument,float(price),float(qty),side,float(price),group_date,customer,settlement_ccy]
        row = dict(zip(self.cols,row_data))
        self.positions.loc[self.positions.shape[0]+1] = row
        with open(f'{data_path}/customer_position.pkl','wb') as fw:
            pickle.dump(self.positions, fw)

        #add new customer into customers.json 
        with open(f'{data_path}/customers.json', "rb") as fw:
            customers = json.load(fw)

        if not customer in customers:
            customers[customer] = {'active':True, 'equity':10}

        with open(f'{data_path}/customers.json', 'w') as fw:
            json.dump(customers, fw)

        self.intercom.emit(IntercomScope.OptionPosition,IntercomChannel.PositionUpdate,[instrument,float(qty),side,float(price),group_date,settlement_ccy,request_id])
        return True

    def change_auto_hedge(self,client_id,args):
        reqeust_id = args[1]
        client_requests[reqeust_id] = {'delta':False,'gamma_vega':False}
        self.intercom.emit(IntercomScope.OptionPosition,IntercomChannel.AutoHedgeUpdate,args)
        return True

    def change_target(self,client_id,args):
        with open(f'{data_path}/delta_data.pkl','rb') as fw:
            delta_data = pickle.load(fw) 
        delta_data['account_target'] = args
        with open(f'{data_path}/delta_data.pkl','wb') as fw:
            pickle.dump(delta_data, fw)
        self.intercom.emit(IntercomScope.OptionPosition,IntercomChannel.TargetUpdate,args)
        return True

    def heart_check(self,client_id,args):
        return True

# Called for every client connecting (after handshake)
def _new_client(client, server):
     clients[client['id']] = client
     print("New client connected and was given id %d" % client['id'])
	#server.send_message_to_all("Hey all, a new client has joined us")


# Called for every client disconnecting
def _client_left(client, server):
    del clients[client['id']]
    print("Client(%d) disconnected" % client['id'])
    

# Called when a client sends a message
def _message_received(client, server, message):
    #print(message)
    if message!=None:
        request = json.loads(message)
        data = getattr(BS, request['func'])(client['id'],request['args'])
        respond = {'func':f"on_{request['func']}", 'data':data}
        respond = str(json.dumps(respond))
        server.send_message(client,respond)
 

def listen_redis_reponse():
    for item in pubsub.listen():
        try:
            channel = bytes.decode(item["channel"])
            func = channel.split(":")[1]
            data = bytes.decode(item["data"])
            data = json.loads(data)
            if data['type'] == "user_positions":
                data['user_positoins'],data['hedge_positions']=get_positions_tree(data)
                del data['quotes']
            elif data['type'] == "auto_hedge":
                greek = data['greek']
                request_id = data['request_id']
                client_requests[request_id][greek] = True
                if sum(client_requests[request_id].values()) == 2:
                    data['result'] = True
                else:
                    data['result'] = False

            for key in clients:
                client = clients[key]
                #request_id = data['request_id']
                respond = {'func':f'on_{func}', 'data':data}
                respond = str(json.dumps(respond, cls=NpEncoder))
                server.send_message(client,respond)       
        except:
            print(traceback.format_exc())
            

def get_positions_tree(data):
    #quote_df = pd.DataFrame(data['quotes']).T.reset_index() 
    quote_df = pd.DataFrame(data['quotes']).T
    del quote_df['avg_price']
    del quote_df['group']
    del quote_df['settlement_ccy']
    user_positions,is_removed = remove_expired_positions(BS.positions)
    user_positions = user_positions.merge(quote_df,left_on='symbol',right_on='symbol')
    user_positions['pnl'] = (user_positions['price'] - user_positions['avg_price'])*user_positions['side']*user_positions['qty']
    user_positions['pnl']=user_positions['pnl'].apply(lambda x:round(x,4))
    pg = user_positions[postion_cols].groupby('customer')
    user_id = user_positions.shape[0]
    group_positoins = []
    for group in pg.groups:
        children = pg.get_group(group)
        pnls = children['pnl'].sum()
        user_positoins = {'index':user_id, 'customer':group, 'pnl':round(pnls,4)}
        user_positoins['children'] = list(children.reset_index().T.to_dict().values())
        group_positoins.append(user_positoins)
        user_id += 1

    #hedged_positoins = []

    return group_positoins,data['hedged_positoins']


if __name__ == '__main__': 
    data_path = '/home/justin/repos/pdt_option/data'
    task_list = []
    clients = {}    
    client_requests = {}
    postion_cols = ['customer','symbol','qty','side','avg_price','price','pnl','delta','gamma','vega','theta']
    PORT=8082
    HOST="0.0.0.0"
    try:
        intercom = Intercom()
        BS = BookingService(intercom)
        pubsub = intercom.subscribe(f'{IntercomScope.Strategy}:{IntercomChannel.StrategyResponse}')
        Thread(target=listen_redis_reponse).start()
        
        server = WebsocketServer(PORT,HOST)
        server.set_fn_new_client(_new_client)
        server.set_fn_client_left(_client_left)
        server.set_fn_message_received(_message_received)
        server.run_forever()
        
    except Exception as e:
        print(e)
