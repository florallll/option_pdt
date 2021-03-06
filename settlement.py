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

# pre_start：交易（回测）开始前运行
def pre_start(context, **kwargs):
    context.logger.info(f'DemoStrategy::run before trading start: {kwargs}')
    context.positions = kwargs['positions']
    # context.bid = {'BTCUSD': 0, 'ETHUSD':0}
    # context.ask = {'BTCUSD': 0, 'ETHUSD':0}
    context.index = {'BTCUSD': 0, 'ETHUSD':0}
    context.edp = {'BTCUSD': 0, 'ETHUSD':0}
    context.min_tick = EXCHANGE_CONFIGS['Apollo']['TICK_SIZE']
    context.set_interval(10, context.settle_start)
    context.settle_dict = {}
    context.price = {}
    context.dt = int(kwargs['time_delta'])
    context.buffer = kwargs['buffer']

def on_position_ready(context, data):
    if data['exchange'] not in ['Apollo','Deribit']:
        context.logger.error(data['exchange'])

def on_signal_ready(context, data):
    pass
    
def on_market_data_ticker_ready(context, data):
    if data['contract_type'] == 'option':
        context.logger.info(f'inside ticker {data["instrument_id"]}')
        context.logger.info(f'inside ticker {data}')
        
def on_market_data_orderbook_ready(context, data):
    with context.lock:
        context.price[data['symbol']] = {
            'buy' : np.array(data['metadata']['asks']).T[0].mean(),
            'sell' : np.array(data['metadata']['bids']).T[0].mean()
        }
        
def on_market_data_index_ready(context, data):
    with context.lock:
        context.index[data['symbol']] = data['metadata']['index']

def on_market_data_estimated_expiration_price_ready(context, data):
    with context.lock:
        context.edp[data['symbol']] = data['metadata']['price']
        
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
    params = ['_id','instrument','currency','strike','cp','qty']
    [_id,instrument,currency,strike,cp,qty] = [settle_object[_] for _ in params]
    context.logger.info(f'in twap {_id}')
    twap_interval = (1800 - datetime.now().minute*60 - datetime.now().second)
    # print('twap_interval',twap_interval)
    dt = int(twap_interval/(qty/context.min_tick[f'{currency}|spot'])) if qty/(twap_interval/context.dt) < context.min_tick[f'{currency}|spot'] else context.dt
    p_list = []
    i = 1
    ptwap = 0
    while datetime.now().hour < 16:
        with context.lock:
            p = context.index[currency]
            if p > 0:
                p_list.append(p) 
            ptwap = np.mean(p_list)
            context.logger.info(f'p:{p} ptwap:{ptwap} i:{i} p_list: {p_list} dt:{dt}')
                    
        if strike < min(p, ptwap):
            dq = {'C': max(qty*(dt*i/twap_interval)-context.settle_dict[_id]['cumu'],0) ,'P': max(context.settle_dict[_id]['cumu'],0)}
            context.send_order(instrument,'sell',context.price[currency]['sell'], dq[cp], 'fak',note = _id)
        elif strike > max(p, ptwap):
            dq = {'P': max(qty*(dt*i/twap_interval)-context.settle_dict[_id]['cumu'],0) ,'C': max(context.settle_dict[_id]['cumu'],0)}
            context.send_order(instrument,'buy',context.price[currency]['buy'], dq[cp], 'fak',note = _id)
        else:
            continue
        i += 1
        context.logger.info(f'time is {datetime.now()} {_id} cumulative exercised amount is {context.settle_dict[_id]["cumu"]}')
        sleep(dt)
    
    if datetime.now().hour == 16 : 
        
        if ptwap > strike and context.index[currency] < (strike - context.buffer[currency]):
            dq = max(context.settle_dict[_id]['cumu']/i,0)
            context.logger.info(f'time is {datetime.now()} index price is {context.index[currency]}')
            context.logger.info(f'unsettling {_id} cumulative exercised amount {context.settle_dict[_id]["cumu"]}')
            while context.settle_dict[_id]['cumu'] > 0 and context.index[currency] < (strike - context.buffer[currency]):
                context.send_order(instrument,'buy',context.ask[currency], dq, 'fak',note = _id)
                sleep(dt)
            if context.settle_dict[_id]['cumu'] > 0 and context.index[currency] >= strike :
                context.send_order(instrument,'buy',context.ask[currency], context.settle_dict[_id]['cumu'], 'fak',note = _id)
                context.logger.info(f'time is {datetime.now()} {_id} cumulative exercised amount is {context.settle_dict[_id]["cumu"]}')
        elif context.settle_dict[_id]['cumu'] < context.settle_dict[_id]['qty'] :
            dq = context.settle_dict[_id]['qty'] - context.settle_dict[_id]['cumu']
            context.send_order(instrument,'buy',context.ask[currency], dq, 'fak',note = _id)
        else:
            context.logger.info(f'time is {datetime.now()} {_id} delivery amount is {context.settle_dict[_id]["cumu"]}')
            # context.settle_started = False
            
def pre_twap(context):
    context.logger.info(f'pre beginning settlement process at {datetime.now()}')
    with context.lock:
        edp = context.edp
        option_df  = context.positions
        option_df['row_id'] = option_df.index
    
    context.logger.info(f'Estimated delivery price now is {edp}')
    # today = date.today().strftime("%Y%m%d")
    today = '20200925'
    
    new_instruments = []
    syms = ['BTCUSD-20200912-10000-C','BTCUSD-20200912-10000-P']
    [new_instruments.append(f"Deribit|{sym}|option|ticker") for sym in syms]
    if hasattr(context, "pubsub"):
        context._add_subscritions(IntercomScope.Market, new_instruments)

    try:
        pre_selection = option_df[(option_df['group'] == today ) & (option_df['settlement'] != 'Cash')]
        if not pre_selection.empty:
            pre_selection['currency'] = pre_selection['symbol'].apply(lambda x: x.split('-')[0])
            pre_selection['strike'] = pre_selection['symbol'].apply(lambda x: float(x.split('-')[-2]))
            pre_selection['cp'] = pre_selection['symbol'].apply(lambda x: x.split('-')[-1])
            
            buyer,seller = (pre_selection['side'] < 0) ,(pre_selection['side'] > 0)
            calls,puts = (pre_selection['cp'] == 'C'),(pre_selection['cp'] == 'P')
            for currency in ['BTCUSD','ETHUSD']:
                currency_label = (pre_selection['currency'] == currency)
                pre_selection.loc[currency_label,'diff'] = edp[currency] - pre_selection['strike']
            diff = (pre_selection['diff'] > 0)
            buy_side = pre_selection[(calls & seller & diff )|(puts & buyer & (- diff))]
            sell_side = pre_selection[(calls & buyer & diff)|(puts & seller & (- diff))]
            
            if not settlements.empty:
                settlements = settlements.to_dict(orient='records')
                for s in settlements:
                    _id = '-'.join([s['customer'],str(s['row_id']),s['symbol']])
                    settle_object = {
                        '_id' : _id,
                        'instrument':f'Apollo|{s["currency"]}|spot',
                        'currency':s['currency'],
                        'strike' : s['strike'],
                        'cp':s['cp'],
                        'qty':s['qty'],
                        'cumu':0,
                        'after_expiry': False,
                        'side':'buy'
                    }
                    if _id not in context.settle_dict.keys():
                        context.settle_dict[_id] = settle_object
                        Thread(target = context.twap,args = [_id]).start()
        else:
            context.logger.info(f'Today {date.today()} no physical settlement available ')
    except:
        # context.logger.error(option_df)
        pass
        
def settle_start(context):
    now = datetime.now()
    context.logger.info(f'inside settle start {now}')
    if now.hour == 19 and now.minute >= 1 :
        context.pre_twap()

    
    


