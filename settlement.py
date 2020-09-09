import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
import pickle
from time import sleep
import traceback
from utils.util_func import *
from configs.typedef import IntercomScope, IntercomChannel
from datetime import datetime,date
from configs import EXCHANGE_CONFIGS
from threading import Thread
import schedule

# pre_start：交易（回测）开始前运行
def pre_start(context, **kwargs):
    context.logger.info(f'DemoStrategy::run before trading start: {kwargs}')
    context.positions = kwargs['positions']
    # context.bid = {'BTCUSD': 0, 'ETHUSD':0}
    # context.ask = {'BTCUSD': 0, 'ETHUSD':0}
    context.index = {'BTCUSD': 0, 'ETHUSD':0}
    context.min_tick_size = EXCHANGE_CONFIGS['Apollo']['TICK_SIZE']
    context.set_interval(10, context.settle_start)
    context.settle_dict = {}
    context.dt = int(kwargs['time_delta'])
    context.buffer = kwargs['buffer']
    # context.settle_started = False

def on_position_ready(context, data):
    if data['exchange'] not in ['Apollo','Deribit']:
        context.logger.error(data)

def on_signal_ready(context, data):
    pass
    
def on_market_data_orderbook_ready(context, data):
    with context.lock:
        context.price[data['symbol']] = {
            'buy' : np.array(data['metadata']['asks']).T[0].mean(),
            'sell' : np.array(data['metadata']['bids']).T[0].mean()
        }
        
def on_market_data_index_ready(context, data):
    with context.lock:
        context.index[data['symbol']] = data['metadata']['index']
    
# on_response_{action}_ready: 处理response的回调函数
def on_response_place_order_ready(context, data):
    context.logger.info(f'DemoStrategy::send order response received')
    metadata = data['metadata']
    error = "error_code" in metadata['metadata']
    result = metadata['metadata']['result']
    _id = metadata['request']['metadata']['note']
    with context.lock:
        if result :
            qty = metadata['request']['metadata']['quantity']
            if context.settle_dict[_id]['after_expiry']:
                context.settle_dict[_id]['cumu'] -= qty
            else:
                context.settle_dict[_id]['cumu'] += qty
            
        if error:
            context.logger.error(f"place order error:{metadata['metadata']['error_code_msg']}")
        else:
            context.logger.info(metadata)
        

def twap(context,_id):
    settle_object = context.settle_dict[_id]
    params = ['_id','instrument','base_currency','strike','cp','qty']
    [_id,instrument,base_currency,strike,cp,qty] = [settle_object[_] for _ in params]
    context.logger.info(f'in twap {_id}')
    dt = context.dt
    if qty/(1800/dt) < context.min_tick_size[f'{base_currency}|spot']:
        dt = int(1800/(qty/context.min_tick_size[f'{base_currency}|spot']))
        
    p_list = []
    i = 1
    ptwap = 0
    while datetime.now().hour < 16:
        with context.lock:
            p = context.index[base_currency]
            if p > 0:
                p_list.append(p) 
            ptwap = np.mean(p_list)
            context.logger.info(f'p:{p} ptwap:{ptwap} i:{i} p_list: {p_list} dt:{dt}')
            
        dq = {'C': max(qty*(dt*i/1800)-context.settle_dict[_id]['cumu'],0) ,'P': max(context.settle_dict[_id]['cumu'],0)}
        
        if strike < min(p, ptwap):
            context.send_order(instrument,'sell',context.price[base_currency]['sell'], dq[cp], 'fak',note = _id)
        elif strike > max(p, ptwap):
            context.send_order(instrument,'buy',context.price[base_currency]['buy'], dq[cp], 'fak',note = _id)
        else:
            continue
        i += 1
        context.logger.info(f'time is {datetime.now()} {_id} cumulative exercised amount is {context.settle_dict[_id]["cumu"]}')
        sleep(dt)
    
    if datetime.now().hour == 16 : 
        
        if ptwap > strike and context.index[base_currency] < (strike - context.buffer[base_currency]):
            dq = max(context.settle_dict[_id]['cumu']/i,0)
            context.logger.info(f'time is {datetime.now()} index price is {context.index[base_currency]}')
            context.logger.info(f'unsettling {_id} cumulative exercised amount {context.settle_dict[_id]["cumu"]}')
            while context.settle_dict[_id]['cumu'] > 0 and context.index[base_currency] < (strike - context.buffer[base_currency]):
                context.send_order(instrument,'buy',context.ask[base_currency], dq, 'fak',note = _id)
                sleep(dt)
            if context.settle_dict[_id]['cumu'] > 0 and context.index[base_currency] >= strike :
                context.send_order(instrument,'buy',context.ask[base_currency], context.settle_dict[_id]['cumu'], 'fak',note = _id)
                context.logger.info(f'time is {datetime.now()} {_id} cumulative exercised amount is {context.settle_dict[_id]["cumu"]}')
        elif context.settle_dict[_id]['cumu'] < context.settle_dict[_id]['qty'] :
            dq = context.settle_dict[_id]['qty'] - context.settle_dict[_id]['cumu']
            context.send_order(instrument,'buy',context.ask[base_currency], dq, 'fak',note = _id)
        else:
            context.logger.info(f'time is {datetime.now()} {_id} delivery amount is {context.settle_dict[_id]["cumu"]}')
            # context.settle_started = False
            
                    
        
def pre_twap(context):
    # print(context.settle_started)
    # if context.settle_started :
    #     return
    context.logger.info(f'pre beginning settlement process at {datetime.now()}')
    with context.lock:
        before_index = context.index
        option_df  = context.positions
    context.logger.info(f'index price now is {before_index}')
        # today = date.today().strftime("%Y%m%d")
    today = '20200925'
    
    try:
        settlements = option_df[(option_df['group'] == today ) & (option_df['side'] < 0) & (option_df['settlement'] != 'Cash')]
        if not settlements.empty:
            settlements['base_currency'] = settlements['symbol'].apply(lambda x: x.split('-')[0])
            settlements['strike'] = settlements['symbol'].apply(lambda x: float(x.split('-')[-2]))
            settlements['cp'] = settlements['symbol'].apply(lambda x: x.split('-')[-1])
            btc_calls = (settlements['base_currency'] == 'BTCUSD')&(settlements['cp'] == 'C') & (settlements['strike'] < before_index['BTCUSD'])
            btc_puts = (settlements['base_currency'] == 'BTCUSD')&(settlements['cp'] == 'P') & (settlements['strike'] > before_index['BTCUSD'])
            eth_calls = (settlements['base_currency'] == 'ETHUSD')&(settlements['cp'] == 'C') & (settlements['strike'] < before_index['ETHUSD'])
            eth_puts = (settlements['base_currency'] == 'ETHUSD')&(settlements['cp'] == 'P') & (settlements['strike'] < before_index['ETHUSD'])
            settlements = settlements[btc_calls | btc_puts | eth_puts | eth_calls]
            if not settlements.empty:
                settlements = settlements.to_dict(orient='records')
                i = 0
                for s in settlements:
                    _id = '-'.join([s['customer'],str(i),s['symbol']])
                    settle_object = {
                        '_id' : _id,
                        'instrument':f'Apollo|{s["base_currency"]}|spot',
                        'base_currency':s['base_currency'],
                        'strike' : s['strike'],
                        'cp':s['cp'],
                        'qty':s['qty'],
                        'cumu':0,
                        'after_expiry': False
                    }
                    i += 1
                    context.settle_dict[_id] = settle_object
                    Thread(target = context.twap,args = [_id]).start()
                context.logger.info(settlements)
            # context.settle_started = True
        else:
            context.logger.info(f'Today {date.today()} no physical settlement available ')
    except:
        context.logger.error(option_df)
        
def settle_start(context):
    now = datetime.now()
    context.logger.info(f'inside settle start {now}')
    if [now.hour,now.minute] == [17,59]:
        context.pre_twap()

    
    


