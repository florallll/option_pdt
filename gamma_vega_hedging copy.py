from utils.util_func import get_human_readable_timestamp
from configs.typedef import TradeSide, OrderType, Direction,OrderStatus
import numpy as np
import pandas as pd
import math
import pickle
import traceback
from utils.util_func import *
from configs.typedef import IntercomScope, IntercomChannel
from datetime import datetime
from utils.SABRLib import *
from utils.OptionUtil import *

template = "gamma_vega_template"

# pre_start：交易（回测）开始前运行
def pre_start(context, **kwargs):
    context.logger.info(f'DemoStrategy::run before trading start: {kwargs}')
    context.hedge_time = kwargs['hedge_time']
    positions_df = kwargs['positions']
    context.positions = get_agg_positions(positions_df)
    context.hedge_positions = {'BTCUSD':{}, 'ETHUSD':{}}
    context.time_limit = kwargs['time_limit']
    context.gamma_limit = kwargs['gamma_limit']
    context.vega_limit = kwargs['vega_limit']
    #context.set_interval(6, context.self_check)
    context.set_interval(5, context.hedge_greek)
    context.set_interval(2, context.check_open_order)
    
 
def on_market_data_1min_ready(context, data):
    pass
     
def on_market_data_trade_ready(context, data):
    context.logger.info("inside marketdata trade ready")
    
def on_market_data_orderbook_ready(context, data):
    context.logger.info("inside marketdata orderbook ready")

def on_market_data_ticker_ready(context, data):
    #context.logger.info(f'inside ticker {data}')
    key = data['symbol']
    if 'instrument_id' in data:
        key = data['instrument_id']
    context.symbol_dict[key] = data['metadata']
    context.symbol_dict[key]['timestamp'] = data['timestamp']
   

def on_market_data_summaryinfo_ready(context, message):
    pass
    '''
    today = datetime.now()
    data = message['metadata']['summary_info']
    summary = pd.DataFrame(data)
    option_df = summary.dropna()
    option_list = [[sym.split('-')[1],sym.split('-')[2],sym.split('-')[3]] \
            for sym in option_df['instrument_id']]
    option_list = np.array(option_list)
    option_df['EXP_DATE'] = option_list[:,0]
    option_df['K'] = option_list[:,1].astype(float)
    option_df['cp'] = option_list[:,2]
    option_df['S'] = option_df['underlying_price']
    option_df['TTM'] = [days_diff(exp_date,today) for exp_date in option_df['EXP_DATE']]
    option_df.to_csv('data/option_df.csv')
    #optExp = pd.Series(option_df['EXP_DATE'].values.ravel()).unique()
    #context.exp_list = [datetime.strptime("{}".format(d), '%d%b%y') for d in optExp]
    
    cols = ['EXP_DATE','ask_price', 'bid_price', 'creation_timestamp','instrument_name', 'K','S','cp',
                            #'interest_rate','open_interest','underlying_index', 'volume','TTM']
    context.all_options = option_df[cols]
    option_df = option_df[cols]
    option_df = option_df[option_df['TTM']>0.01]
    points = get_calibration_contracts(option_df)
    subscription_list = [symbol2subs(symbol,"%d%b%y") for symbol in points['instrument_name']]
    subscription_list.append('Deribit|BTCUSD|perp|trade')
    context._add_subscritions(IntercomScope.Market,list(set(subscription_list)))
    '''


def on_response_inspect_order_ready(context, data):
        context.logger.info('inside inspect order')
        context.logger.info(data) 
        error = "error_code" in data['metadata']['metadata']
        with context.lock:
            if not error:
                context.update_order(data['metadata'])
            else:
                context.logger.info(f"inspect order error {data['metadata']['metadata']['error_code_msg']}")


def on_response_cancel_order_ready(context, data):
        context.logger.info("inside cancel order")
        context.logger.info(data)
        symbol_df = pd.DataFrame(context.symbol_dict).T
        metadata = data['metadata']['request']['metadata']
        instrument_id = metadata['instrument_id']
        market_data = symbol_df.loc[instrument_id]
        error = "error_code" in data['metadata']['metadata']
        with context.lock:
            if not error:
                context.update_order(data['metadata'])
                #order_type = metadata['note']['aggresive']
                diff = metadata['diff']
                context.order_id = None
                context.send_limit_order(instrument_id,market_data,diff,note=metadata['note'])
            else:
                context.logger.info(f"cancel order error:{data['metadata']['metadata']['error_code_msg']}")
                exchange = metadata['exchange']
                #symbol = metadata['instrument_id']
                contract_type = metadata['contract_type']
                order_id = metadata['order_id']
                instrument = '|'.join([exchange, instrument_id, contract_type])
                context.inspect_order(instrument,order_id,note=metadata['note'])
   

def on_position_booking(context,data):
    instrument,qty,side,group_date,request_id = data
    with context.lock:
        if instrument in context.positions:
            current_qty = context.positions[instrument]['net_qty']
            net_qty = qty * side + current_qty
            context.positions[instrument]['net_qty'] = net_qty
        else:
            currency,exp_date,strike,cp = instrument.split("-")
            if currency == "BTCUSD":
                vol_SABR = context.BTC_vol_SABR
            else:
                vol_SABR = context.ETH_vol_SABR
            
            bitYield = vol_SABR['yieldCurve']['rate']
            ttm = vol_SABR['yieldCurve']['tenor']
            fwd = vol_SABR['fwdCurve']['rate'][0]
            exp = datetime.strptime("{}".format(exp_date+" 16:00:00"), "%Y%m%d %H:%M:%S")
            dtm = ((exp-datetime.now()).days+(exp-datetime.now()).seconds/3600/24)/365
            vol = get_vol(vol_SABR, dtm, float(strike))
            fwdYield=interp(dtm,ttm, bitYield)
            fwd_price = fwd*math.exp(fwdYield*dtm)
            eu_option=BSmodel(float(strike), py2ql_date(exp), cp, 'forward')
            eu_option.price(py2ql_date(datetime.now()), fwd_price, vol, 0)
            view = eu_option.view()
            view['price'] = view['price']/fwd
            context.positions[instrument] = {'net_qty':qty * side, 'group':group_date}
            context.positions[instrument].update(view)

    option_df = pd.DataFrame(context.positions).T
    hedge_option_df = pd.DataFrame(context.hedge_positions['BTCUSD']).T
    context.baskets,instruments = context.generate_basket(option_df,context.hedge_positions['BTCUSD'])
    #subscribe instruments if not already
    new_instruments = [[sym,f"Deribit|{sym}|option|ticker"] for sym in instruments if sym not in context.subscription_list]
    new_instruments = np.array(new_instruments)
    if len(new_instruments.shape)==2:
        context.subscription_list.extend(new_instruments[:,0])
        context._add_subscritions(IntercomScope.Market,list(new_instruments[:,1]))
    context.logger.info(f'after positions booked:{context.positions}')
    
# on_signal_ready: 处理signal_data的回调函数
def on_signal_ready(context, data):
    context.logger.info(f'DemoStrategy::signal received')
    try:
        data['volDate'] = datetime.strptime(data['volDate'], "%Y%m%d")
        if data['currency'] == "BTC":
            context.BTC_vol_SABR = data
        else:
            context.ETH_vol_SABR = data
        bitYield = data['yieldCurve']['rate']
        ttm = data['yieldCurve']['tenor']
        fwd = data['fwdCurve']['rate'][0]

        for key in context.positions:
            currency,exp,strike,cp = key.split("-")
            if currency.find(data['currency'])!=-1:
                exp = datetime.strptime("{}".format(exp+" 16:00:00"), "%Y%m%d %H:%M:%S")
                dtm = ((exp-datetime.now()).days+(exp-datetime.now()).seconds/3600/24)/365
                vol = get_vol(data, dtm, float(strike))
                fwdYield=interp(dtm,ttm, bitYield)
                fwd_price = fwd*math.exp(fwdYield*dtm)
                '''
                eu_option=BSmodel(float(strike), py2ql_date(exp), cp, 'forward')
                eu_option.price(py2ql_date(datetime.now()), fwd_price, vol, 0)
                view = eu_option.view()
                view['price'] = view['price']/fwd
                '''
                view = {}
                eu_option = BS([fwd_price, float(strike), 0, dtm*365], volatility=vol*100)
                
                price = eu_option.callPrice if cp == "C" else eu_option.putPrice
                view['price'] = round(price/fwd,4)
                delta = eu_option.callDelta if cp == "C" else eu_option.putDelta
                view['delta'] = round(delta,4)
                theta = eu_option.callTheta if cp == "C" else eu_option.putTheta
                view['theta'] = round(theta,4)
                view['gamma'] = round(eu_option.gamma,6)
                view['vega'] = round(eu_option.vega,4)

                context.positions[key].update(view)

        for base in context.hedge_positions:
            #hedge_df = pd.DataFrame(context.hedge_positions[base]).T
            for key in context.hedge_positions[base]:  
                currency,exp,strike,cp = key.split('_')[0].split('-')
                if currency.find(data['currency'])!=-1:
                    exp = datetime.strptime("{}".format(exp+" 16:00:00"), "%Y%m%d %H:%M:%S")
                    dtm = ((exp-datetime.now()).days+(exp-datetime.now()).seconds/3600/24)/365
                    vol = get_vol(data, dtm, float(strike))
                    fwdYield=interp(dtm,ttm, bitYield)
                    fwd_price = fwd*math.exp(fwdYield*dtm)
                    '''
                    eu_option=BSmodel(float(strike), py2ql_date(exp), cp, 'forward')
                    eu_option.price(py2ql_date(datetime.now()), fwd_price, vol, 0)
                    view = eu_option.view()
                    view['price'] = view['price']/fwd
                    '''
                    view = {}
                    eu_option = BS([fwd_price, float(strike), 0, dtm*365], volatility=vol*100)
                    
                    price = eu_option.callPrice if cp == "C" else eu_option.putPrice
                    view['price'] = round(price/fwd,4)
                    delta = eu_option.callDelta if cp == "C" else eu_option.putDelta
                    view['delta'] = round(delta,4)
                    theta = eu_option.callTheta if cp == "C" else eu_option.putTheta
                    view['theta'] = round(theta,4)
                    view['gamma'] = round(eu_option.gamma,4)
                    view['vega'] = round(eu_option.vega,4)
                    #for col,value in view.items():
                         #hedge_df.loc[key,col]=value
                    context.hedge_positions[base][key].update(view)
            #context.hedge_positions[base] = hedge_df.T.to_dict()

        context.greek_updated = True
    except:
        #context.logger.info(f"Error - volData is:{data}")
        context.logger.error(traceback.format_exc())

# on_response_{action}_ready: 处理response的回调函数
def on_response_place_order_ready(context, data):
    context.logger.info(f'DemoStrategy::send order response received')
    context.logger.info(f'in place order {data}')
    metadata = data['metadata']
    instrument = metadata['request']['metadata']['instrument_id']
    error = "error_code" in metadata['metadata']
    with context.lock:
        if not error:
            del context.orders_existed[instrument]
            symbol = metadata['symbol'] if metadata['request']['metadata']['instrument_id']=="" else metadata['request']['metadata']['instrument_id']
            key = symbol+'_option'
            #context.local_hedge_info[key] =  metadata['request']['metadata']['note']
            context.update_order(metadata)
        else:
            context.logger.info(f"place order error:{metadata['metadata']['error_code_msg']}")

def on_order_ready(context, data):
    pass
    '''
    context.logger.info('inside order update')
    context.logger.info(data)
    order_id = data['metadata']['order_id']
    with context.lock:
        if not order_id in context.orders:
            context.update_order(data)
            context.update_position(data)
        elif not (context.orders[order_id]['status'] == OrderStatus.Filled or context.orders[order_id]['status'] == OrderStatus.Cancelled):
            context.update_order(data)
            context.update_position(data)
    '''


# on_position_ready: 处理position_info的回调函数
def on_position_ready(context, data):
    posInfoType = data['metadata']['posInfoType']
    
    if posInfoType=="option_position":
        hedge_positions = data['metadata']['metadata']
        for col in context.position_del_cols:
            del hedge_positions[col]

        currency = data['metadata']['symbol']
        cleared_positions = [key for key in context.hedge_positions[currency] if key not in hedge_positions]
        
        for key in cleared_positions:
            context.hedge_positions[currency][key].update(context.empty_pos)

        for key,value in hedge_positions.items():
            option_position = {}
            currency,exp,strike,cp = key.split('_')[0].split('-')
            if value['buy_available']>0:
                option_position['side'] = 1
                option_position['qty'] = value['buy_available']
                option_position['group'] = exp
            else:
                option_position['side'] = -1
                option_position['qty'] = value['sell_available']
                option_position['group'] = exp
 
            if key in context.hedge_positions[currency]:
                context.hedge_positions[currency][key].update(option_position)
            else:
                context.hedge_positions[currency][key] = option_position

            #if key in context.local_hedge_info:
                #context.hedge_positions[currency][key].update(context.local_hedge_info[key])
    
        if not context.basket_update and currency=="BTCUSD":
            option_df = pd.DataFrame(context.positions).T
            #hedge_option_df = pd.DataFrame(context.hedge_positions[currency]).T
            context.baskets,instruments = context.generate_basket(option_df,context.hedge_positions["BTCUSD"])
            if len(instruments)>0:
                new_instruments = [[sym,f"Deribit|{sym}|option|ticker"] for sym in instruments if sym not in context.subscription_list]
                new_instruments = np.array(new_instruments)
                if len(new_instruments.shape)==2:
                    context.subscription_list.extend(new_instruments[:,0])
                    context._add_subscritions(IntercomScope.Market,list(new_instruments[:,1]))
                context.basket_update = True
 
def on_auto_hedge(context,data):
    auto_hedge,request_id = data
    context.logger.info(f'DemoStrategy::on_auto_hedge received:{data}')
    context.auto_hedge = auto_hedge
    context.send_data_to_user({"type":"auto_hedge","request_id":request_id,"greek":'gamma_vega'})

def update_order(context, data):
    metadata = data['request']['metadata']
    if 'options' in metadata:
        del metadata['options']
    order_id = metadata['order_id'] 
    
    if order_id in context.orders:
        context.orders[order_id].update(metadata)
        side = 1 if context.orders[order_id]['direction'].lower()=='buy' else -1
        symbol = context.orders[order_id]['symbol']
        instrument = context.orders[order_id]['instrument_id']
    else:
        context.orders[order_id] = metadata
        side = 1 if metadata['direction'].lower()=='buy' else -1
        symbol = metadata['symbol']
        instrument = metadata['instrument_id']
 
    status = metadata['status']
    if status == OrderStatus.Filled or status == OrderStatus.Cancelled:
        key = instrument + "_option"
        target_qty = metadata['note']['target_qty']
        target_side = 1 if target_qty > 0 else -1
        if key in context.hedge_positions[symbol]:
            context.hedge_positions[symbol][key]['qty'] = abs(target_qty)
            context.hedge_positions[symbol][key]['side'] = target_side
        else:
            context.hedge_positions[symbol][key] = {'qty':abs(target_qty),'side':target_side}

        #context.hedge_positions[symbol][key].update(context.local_hedge_info[key])

        if instrument in context.sent_orders_dict:
            del context.sent_orders_dict[instrument] 
    else:
        context.sent_orders_dict[instrument]  = order_id

    
    order_df = pd.DataFrame(context.orders).T
    order_df = order_df[context.order_cols]
    order_df = order_df.apply(pd.to_numeric, downcast='float',errors='ignore')
    update_kdb(order_df,'orders',context.logger)
    

'''
def update_position(context, data):
    if 'instrument_id' in data:
        symbol = data['symbol'] if data['instrument_id']=="" else data['instrument_id']
    else:
        symbol = data['symbol']
    status = data['order_info']['status']
    if 'direction' in data['metadata']:
        direction = data['metadata']['direction']
    else:
        direction = data['request']['metadata']['direction']
    filled_qty = data['order_info']['filled']
    if status==OrderStatus.Filled or status==OrderStatus.Cancelled:
        if symbol not in context.hedge_positions:
            record = {
                'avg_price':data['order_info']['avg_executed_price'],
                'qty':filled_qty,
                'side':1 if direction.lower()=='buy' else -1,
                'close_price':data['order_info']['avg_executed_price'],
                'group':data['request']['metadata']['note']['group'],
                'greek':data['request']['metadata']['note']['greek']
            }
        else:
            qty = context.hedge_positions[symbol]['qty']
            avg_price = context.hedge_positions[symbol]['avg_price']
            side = context.hedge_positions[symbol]['side']
            new_side = 1 if direction.lower()=='buy' else -1
            new_qty = qty * side + filled_qty * new_side
            if new_qty == 0:
                updated_qty = 0  
                updated_side = side
            elif new_qty > 0:
                updated_qty = abs(new_qty)
                updated_side = 1
            elif new_qty < 0:
                updated_qty = abs(new_qty)
                updated_side = -1

            amount = qty * avg_price + filled_qty * data['order_info']['avg_executed_price']
            total_qty = qty+filled_qty
            if total_qty !=0:
                new_avg_price = amount / total_qty
            else:
                new_avg_price = 0
            record = {
                'avg_price':new_avg_price,
                'qty':updated_qty,
                'side':updated_side,
                'close_price':data['order_info']['avg_executed_price'],  
                'group':data['request']['metadata']['note']['group'],
                'greek':data['request']['metadata']['note']['greek']
            }
        context.hedge_positions[symbol] = record
        
        strategy_data = {'hedge_time':context.hedge_time,'positions':context.positions,'hedge_positions':context.hedge_positions}
        with open(f'data/strategy_data.pkl','wb') as fw:
            pickle.dump(strategy_data, fw)

'''
         
# 其它
# context.set_interval可设定周期性运行的函数
# context.register_handler可以将回调函数注册至指定redis channel
# 待新增
def check_open_order(context):
    context.logger.info(f'check_open_order:{datetime.now()}')
    with context.lock:
        open_orders={key: value for key, value in context.orders.items() if value['status']==OrderStatus.Submitted \
                                        or value['status']==OrderStatus.PartiallyFilled or value['status']==OrderStatus.Unknown}
        for key,value in open_orders.items():
            exchange = value['exchange']
            symbol = value['instrument_id']
            contract_type = value['contract_type']
            instrument = '|'.join([exchange, symbol, contract_type])
            context.inspect_order(instrument,key,note=value['note'])

        #remove any expired positions
        for key in list(context.positions.keys()):
            currency,expiry,strike,cp = key.split("-")
            exp = datetime.strptime("{}".format(expiry+" 16:00:00"), "%Y%m%d %H:%M:%S")
            dtm = ((exp-datetime.now()).days+(exp-datetime.now()).seconds/3600/24)
            if dtm<=0:
                del context.positions[key]


def self_check(context):
    """
    do something
    """
    symbol_df = pd.DataFrame(context.symbol_dict).T
    if len(symbol_df)==len(context.hedge_positions):
        postion_df = pd.DataFrame(context.hedge_positions).T
        symbol_df = symbol_df.join(postion_df)
        options_index = [idx for idx in symbol_df.index if idx != 'BTCUSD']
        option_df = symbol_df.loc[options_index]
        option_delta = (option_df['net_qty'] * option_df['gamma']).sum()
 

def hedge_greek(context):
    start = datetime.now()
    #symbol_df = pd.DataFrame(context.symbol_dict).T
    #time_diff = datetime.now() - context.hedge_time 
    #minutes_diff = time_diff.days * 1440 + time_diff.seconds/60
    option_df = pd.DataFrame(context.positions).T
    hedge_option_df = pd.DataFrame(context.hedge_positions['BTCUSD']).T
    #context.logger.info(f"option_df {option_df},total_gamma:{option_df['gamma']*option_df['net_qty']}")

    account_gamma = 0
    account_vega = 0
    for basket in context.baskets:
        group = basket['group']
        symbol1 = basket['gamma']
        symbol_key1 = symbol1+"_option"
        symbol2 = basket['vega']
        symbol_key2 = symbol2+"_option"
        #clear_list = basket['clear_list']
 
        if symbol_key1 not in context.hedge_positions['BTCUSD']:     
            context.hedge_positions['BTCUSD'][symbol_key1] = {'group':group}

        if symbol_key2 not in context.hedge_positions['BTCUSD']:
            context.hedge_positions['BTCUSD'][symbol_key2] = {'group':group}

        '''
        #先处理需要平的标的
        if len(clear_list)>0:
            for sym in clear_list:
                market_data = context.symbol_dict[sym]
                note_clear = {'aggresive':'maker', 'group':group,'greek':'clear','target_qty':0}
                context.order_execution(sym,market_data,0,note_clear)
        #如果不需要换对冲的标的
        '''
        
        #context.logger.info(f'inside hedge1: {context.symbol_dict}')
        #context.logger.info(f'inside hedge2: {context.hedge_positions}')
        
        if symbol1 not in context.symbol_dict or symbol2 not in context.symbol_dict or 'BTCUSD' not in context.symbol_dict \
        or 'gamma' not in context.hedge_positions['BTCUSD'][symbol_key1] or 'gamma' not in context.hedge_positions['BTCUSD'][symbol_key2] or not context.basket_update:
            continue
        else:
            symbol1_tick= context.symbol_dict[symbol1]
            symbol2_tick = context.symbol_dict[symbol2]
            symbol1_data = hedge_option_df.loc[symbol_key1]
            symbol2_data = hedge_option_df.loc[symbol_key2]
            hedge_group_opiton_df = hedge_option_df[hedge_option_df['group'] == group]
            basket_option_df = option_df[option_df['group'] == group]
            #context.logger.info(f"{basket_option_df['gamma']*option_df['net_qty']}")
            '''
            S = context.symbol_dict['BTCUSD']['last_price']
            
            symbol1_pa_delta = symbol1_data['delta'] - symbol1_tick['mark_price']
            symbol1_pa_gamma = S*symbol1_data['gamma'] - symbol1_pa_delta  
            symbol1_pa_vega = symbol1_data['vega']/S
            symbol2_pa_delta = symbol2_data['delta'] - symbol2_tick['mark_price']
            symbol2_pa_gamma = S*symbol2_data['gamma'] - symbol2_pa_delta  
            symbol2_pa_vega = symbol2_data['vega']/S
            #context.logger.info(f'symbol1_bs_gamma {symbol1_data["gamma"]},symbol1_pa_gamma {symbol1_pa_gamma},symbol2_bs_gamma {symbol2_data["gamma"]},\
            #symbol2_pa_gamma {symbol2_pa_gamma},symbol1_bs_vega {symbol1_data["vega"]},symbol1_pa_vega {symbol1_pa_vega},S {S},')
            
            basket_option_df = option_df[option_df['group'] == group]
            basket_bs_delta = (basket_option_df['net_qty']  * basket_option_df['delta']).sum()
            basket_option_value = (basket_option_df['net_qty'] * basket_option_df['price']).sum()
            basket_pa_delta = basket_bs_delta - basket_option_value  
            basket_bs_gamma = (basket_option_df['net_qty'] * basket_option_df['gamma']).sum() 
            basket_pa_gamma = S*basket_bs_gamma - basket_pa_delta
            basket_bs_vega = (basket_option_df['net_qty'] * basket_option_df['vega']).sum()
            basket_pa_vega = basket_bs_vega/S
            #context.logger.info(f'basket_bs_delta {basket_bs_delta},basket_option_value {basket_option_value},basket_pa_delta {basket_pa_delta},\
            #basket_bs_gamma {basket_bs_gamma},basket_pa_gamma {basket_pa_gamma},basket_bs_vega {basket_bs_vega},basket_pa_vega {basket_pa_vega},')
        
            A = np.array([[symbol1_pa_gamma,symbol2_pa_gamma],[symbol1_pa_vega,symbol2_pa_vega]])
            b = np.array([basket_pa_gamma,basket_pa_vega])
            #context.logger.info(f'this is for A {A},this is for B {b}')
            '''
            try:
                #因为我们是客户的对手方所以需要乘以-1
                basket_bs_gamma = -1*(basket_option_df['net_qty'] * basket_option_df['gamma']).sum() 
                basket_bs_vega = -1*(basket_option_df['net_qty'] * basket_option_df['vega']).sum()

                current_hedge_gamma = (hedge_group_opiton_df['qty'] * hedge_group_opiton_df['side']*hedge_group_opiton_df['gamma']).sum() 
                group_gamma = basket_bs_gamma - current_hedge_gamma
                account_gamma += group_gamma
                current_hedge_vega = (hedge_group_opiton_df['qty'] * hedge_group_opiton_df['side']*hedge_group_opiton_df['vega']).sum() 
                group_vega = basket_bs_vega - current_hedge_vega
                account_vega += group_vega

                A = np.array([[symbol1_data['gamma'],symbol2_data['gamma']],[symbol1_data['vega'],symbol2_data['vega']]])
                b = np.array([group_gamma,group_vega])
                sym1_target_qty,sym2_target_qty = np.linalg.solve(A,b)

                sym1_diff = sym1_target_qty - symbol1_data['qty']*symbol1_data['side']
                sym2_diff = sym2_target_qty - symbol2_data['qty']*symbol2_data['side']
                #sym1_qty = round(sym1_diff,1)
                #sym2_qty = round(sym2_diff,1)
                sym1_qty = int(sym1_diff * 10) / 10
                sym2_qty = int(sym2_diff * 10) / 10

                context.logger.info(f"time takes {datetime.now()-start}")  
                context.logger.info(f"basket_bs_gamma is {basket_bs_gamma},symbol1_gamma is {symbol1_data['gamma']},symbol2_gamma is {symbol2_data['gamma']},basket_bs_vega is {basket_bs_vega},symbol1_vega is {symbol1_data['vega']},symbol2_vega is {symbol2_data['vega']}")
                context.logger.info(f"group_gamma is {group_gamma},group_vega is {group_vega},{symbol1} is {sym1_diff}, {symbol2} is {sym2_diff}")
                note_gamma = {'aggresive':'maker', 'group':group,'greek':'gamma','target_qty':sym1_target_qty,}
                note_vega = {'aggresive':'maker', 'group':group,'greek':'vega','target_qty':sym2_target_qty}
        
                if context.auto_hedge:
                    if  abs(group_gamma)>context.gamma_limit or abs(group_vega)>context.vega_limit :
                        context.logger.info("in limit beach")
                        context.order_execution(symbol1,symbol1_tick,sym1_qty,note_gamma)
                        context.order_execution(symbol2,symbol2_tick,sym2_qty,note_vega)
                        context.hedge_time = datetime.now()
                        strategy_data = {'hedge_time':context.hedge_time}
                        with open(f'data/gamma_data.pkl','wb') as fw:
                            pickle.dump(strategy_data, fw)
                    else:
                        if symbol1 in context.sent_orders_dict:
                            context.logger.info(f"in exsiting order {symbol1}")
                            context.order_execution(symbol1,symbol1_tick,sym1_qty,note_gamma)
                        if symbol2 in context.sent_orders_dict:
                            context.logger.info(f"in exsiting order {symbol2}")
                            context.order_execution(symbol2,symbol2_tick,sym2_qty,note_vega)
                
            except:
                context.logger.error(traceback.format_exc())
                
                
    user_data = {
            'type':'gamma',
            'account_gamma':'%.6f' % account_gamma,
            'account_vega':'%.4f' % account_vega,
    }
    context.send_data_to_user(user_data)   
  

def order_execution(context,instrument,market_data,diff,note):
    with context.lock:
        #instrument = market_data.name
        if diff == 0 or instrument in context.orders_existed:
            return
        #If there is an existing order:
        #If it's a taker order, cancel it directly
        #If it's a maker order, cancel it if the price does not match the best price or the side is changed 
        if instrument in context.sent_orders_dict:
            sent_order_id = context.sent_orders_dict[instrument]
            open_order = context.orders[sent_order_id]
            context.logger.info(f'open_order is {open_order}, market data is {market_data}')
            if open_order['note']['aggresive'] == 'maker':
                order_side = open_order['direction']
                if diff > 0:
                    side = Direction.Buy
                    best_price = market_data['best_bid']
                elif diff < 0:
                    side = Direction.Sell
                    best_price = market_data['best_ask']
                
                context.logger.info(f'inside order_excution, there is an exsting order, side:{side}, order_side:{order_side}, best_price:{best_price},open_price:{open_order["price"]}')
                if side.lower()!=order_side.lower() or best_price!=open_order['price']:
                        context.cancel_order(f'Deribit|{instrument}|option',sent_order_id,diff=diff,note=note) 
            else:
                context.cancel_order(f'Deribit|{instrument}|option',sent_order_id,diff=diff,note=note) 
        else: 
            #If there has no existing order, send order to the market
            context.send_limit_order(instrument,market_data,diff,note=note)