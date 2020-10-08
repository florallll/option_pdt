from utils.util_func import get_human_readable_timestamp
from configs.typedef import TradeSide, OrderType, Direction, OrderStatus
import numpy as np
import pandas as pd
import math
import pickle
import traceback
from scipy import optimize as op
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
    context.hedge_positions = {'BTCUSD': {}, 'ETHUSD': {}}
    context.currency = kwargs['currency']
    context.time_limit = kwargs['time_limit']
    context.gamma_maker_limit = kwargs['gamma_maker_limit']
    context.gamma_taker_limit = kwargs['gamma_taker_limit']
    context.vega_maker_limit = kwargs['vega_maker_limit']
    context.vega_taker_limit = kwargs['vega_taker_limit']
    context.taker_spread = kwargs['taker_spread']
    context.max_qty = kwargs['max_qty']
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
            context.logger.info(
                f"inspect order error {data['metadata']['metadata']['error_code_msg']}")


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
            group = metadata['note']['group']
            send_new = metadata['note']['send_new']
            if instrument_id in context.cancel_syms[group]:
                del context.cancel_syms[group][instrument_id]
            if len(context.cancel_syms[group]) == 0 and send_new:
                for sym in context.new_syms[group]:
                    if not sym in context.orders_existed:
                        market_data['diff'] = context.new_syms[group][sym]['diff']
                        context.logger.info(f"[SENDING NEW SYM] {context.new_syms}")
                        aggresive = context.new_syms[group][sym]['aggresive']
                        note = {'aggresive': aggresive, 'group': group, 'target_qty': context.new_syms[group][
                            sym]['target_qty'], 'target_side': context.new_syms[group][sym]['target_side']}
                        context.send_limit_order(sym, market_data, note)
                        context.logger.info(f"[SENT NEW SYM] {sym}:{note}")
        else:
            context.logger.info(
                f"cancel order error:{data['metadata']['metadata']['error_code_msg']}")
            exchange = metadata['exchange']
            #symbol = metadata['instrument_id']
            contract_type = metadata['contract_type']
            order_id = metadata['order_id']
            instrument = '|'.join([exchange, instrument_id, contract_type])
            context.inspect_order(instrument, order_id, note=metadata['note'])


def on_position_booking(context, data):
    if data['currency'] == context.currency:
        params = ['instrument','qty','side','avg_price','group_date','settlement_ccy','request_id']
        [instrument,qty,side,avg_price,group_date,settlement_ccy,request_id] =  [data[_] for _ in params]


        with context.lock:
            if instrument in context.positions:
                current_qty = context.positions[instrument]['net_qty']
                net_qty = qty * side + current_qty
                context.positions[instrument]['net_qty'] = net_qty
            else:
                currency, exp_date, strike, cp = instrument.split("-")
                bitYield = context.vol_SABR['yieldCurve']['rate']
                ttm = context.vol_SABR['yieldCurve']['tenor']
                fwd = context.vol_SABR['fwdCurve']['rate'][0]
                exp = datetime.strptime("{}".format(
                    exp_date+" 16:00:00"), "%Y%m%d %H:%M:%S")
                dtm = ((exp-datetime.now()).days +
                    (exp-datetime.now()).seconds/3600/24)/365
                vol = get_vol(context.vol_SABR, dtm, float(strike))
                fwdYield = interp(dtm, ttm, bitYield)
                fwd_price = fwd*math.exp(fwdYield*dtm)
                eu_option = BSmodel(float(strike), py2ql_date(exp), cp, 'forward')
                eu_option.price(py2ql_date(datetime.now()), fwd_price, vol, 0)
                view = eu_option.view()
                view['price'] = view['price']/fwd
                context.positions[instrument] = {
                    'net_qty': qty * side, 'group': group_date}
                context.positions[instrument].update(view)

        option_df = pd.DataFrame(context.positions).T
        #hedge_option_df = pd.DataFrame(context.hedge_positions['BTCUSD']).T
        context.baskets, instruments = context.generate_basket(option_df)
        # subscribe instruments if not already
        new_instruments = [[sym, f"Deribit|{sym}|option|ticker"]
                        for sym in instruments if sym not in context.subscription_list]
        new_instruments = np.array(new_instruments)
        if len(new_instruments.shape) == 2:
            context.subscription_list.extend(new_instruments[:, 0])
            context._add_subscritions(
                IntercomScope.Market, list(new_instruments[:, 1]))
        context.logger.info(f'after positions booked:{context.positions}')

# on_signal_ready: 处理signal_data的回调函数


def on_signal_ready(context, data):
    context.logger.info(f'DemoStrategy::signal received')
    try:
        if data['currency'] != context.currency[:3]:
            return 
        data['volDate'] = datetime.strptime(data['volDate'], "%Y%m%d")
        context.vol_SABR = data
        bitYield = data['yieldCurve']['rate']
        ttm = data['yieldCurve']['tenor']
        fwd = data['fwdCurve']['rate'][0]

        for key in context.positions:
            currency, exp, strike, cp = key.split("-")
            if currency.find(data['currency']) != -1:
                exp = datetime.strptime("{}".format(
                    exp+" 16:00:00"), "%Y%m%d %H:%M:%S")
                dtm = ((exp-datetime.now()).days +
                       (exp-datetime.now()).seconds/3600/24)/365
                vol = get_vol(data, dtm, float(strike))
                fwdYield = interp(dtm, ttm, bitYield)
                fwd_price = fwd*math.exp(fwdYield*dtm)
                '''
                eu_option=BSmodel(float(strike), py2ql_date(exp), cp, 'forward')
                eu_option.price(py2ql_date(datetime.now()), fwd_price, vol, 0)
                view = eu_option.view()
                view['price'] = view['price']/fwd
                '''
                view = {}
                eu_option = BS([fwd_price, float(strike), 0,
                                dtm*365], volatility=vol*100)

                price = eu_option.callPrice if cp == "C" else eu_option.putPrice
                view['price'] = round(price/fwd, 4)
                delta = eu_option.callDelta if cp == "C" else eu_option.putDelta
                view['delta'] = round(delta, 4)
                theta = eu_option.callTheta if cp == "C" else eu_option.putTheta
                view['theta'] = round(theta, 4)
                view['gamma'] = round(eu_option.gamma, 6)
                view['vega'] = round(eu_option.vega, 4)

                context.positions[key].update(view)

        for base in context.hedge_positions:
            #hedge_df = pd.DataFrame(context.hedge_positions[base]).T
            for key in context.hedge_positions[base]:
                currency, exp, strike, cp = key.split('_')[0].split('-')
                if currency.find(data['currency']) != -1:
                    exp = datetime.strptime("{}".format(
                        exp+" 16:00:00"), "%Y%m%d %H:%M:%S")
                    dtm = ((exp-datetime.now()).days +
                           (exp-datetime.now()).seconds/3600/24)/365
                    vol = get_vol(data, dtm, float(strike))
                    fwdYield = interp(dtm, ttm, bitYield)
                    fwd_price = fwd*math.exp(fwdYield*dtm)
                    '''
                    eu_option=BSmodel(float(strike), py2ql_date(exp), cp, 'forward')
                    eu_option.price(py2ql_date(datetime.now()), fwd_price, vol, 0)
                    view = eu_option.view()
                    view['price'] = view['price']/fwd
                    '''
                    view = {}
                    eu_option = BS([fwd_price, float(strike), 0,
                                    dtm*365], volatility=vol*100)

                    price = eu_option.callPrice if cp == "C" else eu_option.putPrice
                    view['price'] = round(price/fwd, 4)
                    delta = eu_option.callDelta if cp == "C" else eu_option.putDelta
                    view['delta'] = round(delta, 4)
                    theta = eu_option.callTheta if cp == "C" else eu_option.putTheta
                    view['theta'] = round(theta, 4)
                    view['gamma'] = round(eu_option.gamma, 4)
                    view['vega'] = round(eu_option.vega, 4)
                    # for col,value in view.items():
                    # hedge_df.loc[key,col]=value
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
            symbol = metadata['symbol'] if metadata['request']['metadata'][
                'instrument_id'] == "" else metadata['request']['metadata']['instrument_id']
            key = symbol+'_option'
            #context.local_hedge_info[key] =  metadata['request']['metadata']['note']
            context.update_order(metadata)
        else:
            context.logger.error(
                f"place order error:{metadata['metadata']['error_code_msg']}")


def on_response_modify_order_ready(context, data):
    context.logger.info(f'DemoStrategy::modify order response received')
    context.logger.info(f'in modify order {data}')
    metadata = data['metadata']
    error = "error_code" in metadata['metadata']
    with context.lock:
        if not error:
            context.update_order(metadata)
        else:
            context.logger.error(
                f"Modify order error:{metadata['metadata']['error_code_msg']}")


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

    if posInfoType == "option_position":
        hedge_positions = data['metadata']['metadata']
        for col in context.position_del_cols:
            del hedge_positions[col]

        currency = data['metadata']['symbol']
        cleared_positions = [
            key for key in context.hedge_positions[currency] if key not in hedge_positions]

        for key in cleared_positions:
            context.hedge_positions[currency][key].update(context.empty_pos)

        for key, value in hedge_positions.items():
            option_position = {}
            currency, exp, strike, cp = key.split('_')[0].split('-')
            if value['buy_available'] > 0:
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

            # if key in context.local_hedge_info:
                # context.hedge_positions[currency][key].update(context.local_hedge_info[key])
        '''
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
        '''


def on_auto_hedge(context, data):
    auto_hedge, request_id, currency = data
    if currency == context.currency[:3]:
        context.logger.info(f'DemoStrategy::on_auto_hedge received:{data}')
        context.auto_hedge = auto_hedge
        context.send_data_to_user(
            {"type": "auto_hedge", "request_id": request_id, "greek": 'gamma_vega'})


def update_order(context, data):
    metadata = data['request']['metadata']
    if 'options' in metadata:
        del metadata['options']
    order_id = metadata['order_id']

    if order_id in context.orders:
        context.orders[order_id].update(metadata)
        #side = 1 if context.orders[order_id]['direction'].lower()=='buy' else -1
        symbol = context.orders[order_id]['symbol']
        instrument = context.orders[order_id]['instrument_id']
    else:
        context.orders[order_id] = metadata
        #side = 1 if metadata['direction'].lower()=='buy' else -1
        symbol = metadata['symbol']
        instrument = metadata['instrument_id']

    exp_date = instrument.split('-')[1]
    status = metadata['status']
    if status == OrderStatus.Filled or status == OrderStatus.Cancelled:
        key = instrument + "_option"
        target_qty = metadata['note']['target_qty']
        target_side = metadata['note']['target_side']
        if key in context.hedge_positions[symbol]:
            context.hedge_positions[symbol][key]['qty'] = target_qty
            context.hedge_positions[symbol][key]['side'] = target_side
        else:
            context.hedge_positions[symbol][key] = {
                'qty': target_qty, 'side': target_side}

        # context.hedge_positions[symbol][key].update(context.local_hedge_info[key])

        if instrument in context.sent_orders_dict[exp_date]:
            del context.sent_orders_dict[exp_date][instrument]
    else:
        context.sent_orders_dict[exp_date][instrument] = order_id

    '''
    order_df = pd.DataFrame(context.orders).T
    order_df = order_df[context.order_cols]
    order_df = order_df.apply(pd.to_numeric, downcast='float',errors='ignore')
    update_kdb(order_df,'orders',context.logger)
    '''


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
        open_orders = {key: value for key, value in context.orders.items() if value['status'] == OrderStatus.Submitted
                       or value['status'] == OrderStatus.PartiallyFilled or value['status'] == OrderStatus.Unknown}
        for key, value in open_orders.items():
            exchange = value['exchange']
            symbol = value['instrument_id']
            contract_type = value['contract_type']
            instrument = '|'.join([exchange, symbol, contract_type])
            context.inspect_order(instrument, key, note=value['note'])

        # remove any expired positions
        for key in list(context.positions.keys()):
            currency, expiry, strike, cp = key.split("-")
            exp = datetime.strptime("{}".format(
                expiry+" 16:00:00"), "%Y%m%d %H:%M:%S")
            dtm = ((exp-datetime.now()).days +
                   (exp-datetime.now()).seconds/3600/24)
            if dtm <= 0:
                del context.positions[key]


def self_check(context):
    """
    do something
    """
    symbol_df = pd.DataFrame(context.symbol_dict).T
    if len(symbol_df) == len(context.hedge_positions):
        postion_df = pd.DataFrame(context.hedge_positions).T
        symbol_df = symbol_df.join(postion_df)
        options_index = [idx for idx in symbol_df.index if idx != context.currency]
        option_df = symbol_df.loc[options_index]
        option_delta = (option_df['net_qty'] * option_df['gamma']).sum()


def hedge_greek(context):
    start = datetime.now()
    currency = context.currency
    option_df = pd.DataFrame(context.positions).T
    hedge_option_df = pd.DataFrame(context.hedge_positions[currency]).T
    from_start_to_now = datetime.now() - context.strgy_start_time
    warm_up_time = from_start_to_now.days * 1440 + from_start_to_now.seconds/60
    #context.logger.info(f'hedge_option_df {hedge_option_df},option_df {option_df}')

    context.baskets, instruments = context.generate_basket(option_df)
    #context.logger.info(f'context.baskets {context.baskets},instruments {instruments}')
    if len(instruments) > 0:
        new_instruments = [[sym, f"Deribit|{sym}|option|ticker"]
                           for sym in instruments if sym not in context.subscription_list]
        new_instruments = np.array(new_instruments)
        if len(new_instruments.shape) == 2 and hasattr(context, "pubsub"):
            context.sub_counter += 1
            context.logger.info(f'new_instruments: {new_instruments[:, 1]},sub_counter:{context.sub_counter}')
            context.subscription_list.extend(new_instruments[:, 0])
            context._add_subscritions(
                IntercomScope.Market, list(new_instruments[:, 1]))

    account_gamma = 0
    account_vega = 0
    exposure_gamma = 0
    deribit_gamma = 0
    exposure_vega = 0
    deribit_vega = 0
    for basket in context.baskets:
        group = basket['group']
        data_dict = {}
        gammas = []
        vegas = []
        
        for sym in basket['syms']:
            sym_key = sym+"_option"
            data_dict[sym] = {}
            if not sym_key in context.hedge_positions[currency]:
                context.hedge_positions[currency][sym_key] = {'group': group}
            elif sym in context.symbol_dict and 'gamma' in context.hedge_positions[currency][sym_key]:
                data_dict[sym]['best_bid'] = context.symbol_dict[sym]['best_bid']
                data_dict[sym]['best_ask'] = context.symbol_dict[sym]['best_ask']
                data_dict[sym].update(hedge_option_df.loc[sym_key])
                gammas.append(data_dict[sym]['gamma'])
                vegas.append(data_dict[sym]['vega'])

        context.logger.info(f'group is {group},len(gammas) is {len(gammas)}, len basket is {len(basket["syms"])}')
        if len(gammas) != len(basket['syms']) or len(basket['syms'])==0:
            continue
        else:
            try:
                hedge_group_opiton_df = hedge_option_df[hedge_option_df['group'] == group]
                basket_option_df = option_df[option_df['group'] == group]

                # 因为我们是客户的对手方所以需要乘以-1
                basket_bs_gamma = -1 * \
                    (basket_option_df['net_qty'] * basket_option_df['gamma']).sum()
                basket_bs_vega = -1 * \
                    (basket_option_df['net_qty'] * basket_option_df['vega']).sum()
                exposure_gamma += basket_bs_gamma
                exposure_vega += basket_bs_vega
            
                current_hedge_gamma = (
                    hedge_group_opiton_df['qty'] * hedge_group_opiton_df['side']*hedge_group_opiton_df['gamma']).sum()
                deribit_gamma += current_hedge_gamma
                group_gamma = basket_bs_gamma + current_hedge_gamma
                account_gamma += group_gamma
                current_hedge_vega = (
                    hedge_group_opiton_df['qty'] * hedge_group_opiton_df['side']*hedge_group_opiton_df['vega']).sum()
                deribit_vega += current_hedge_vega
                group_vega = basket_bs_vega + current_hedge_vega
                account_vega += group_vega

                context.logger.info(
                    f"[THIS IS GROUP]:{group}, group_gamma:{group_gamma}, group_vega:{group_vega}")

                if group_gamma >= 0 and group_vega >= 0:
                    target_side = -1
                elif group_gamma < 0 and group_vega < 0:
                    target_side = 1
                else:
                    continue
           
                low_bound_qty = 0.1
                up_bound_qty = abs(basket_option_df['net_qty']).sum()/len(gammas)
                if up_bound_qty < low_bound_qty:
                    low_bound_qty = up_bound_qty
                    up_bound_qty = 0.1
                gs = np.asarray(gammas)
                vs = np.asarray(vegas)/10000
                # Target function is to minimize simple sum of gamma and vega
                target_func = np.append(
                    gs + vs, - abs(group_gamma) - abs(group_vega)/10000)
                # Result gamma is required to be larger than 0
                gamma_constraint = -target_func
                # Result vega is required to be less than 1
                vega_constraint = np.append(vs, - abs(group_vega)/10000)
                A_ub = np.vstack([gamma_constraint, vega_constraint])
                b_ub = np.array([0, 1])
                # A_eq and b_eq is for assigning coefficient x3 of -group_gamma-group_vega to be 1
                # x1*gamma+x2*vega + x3*(-group_gamma-group_vega)
                zeros = np.zeros(len(gs))
                A_eq = np.append(zeros, 1)
                A_eq = A_eq.reshape(1, len(gs)+1)
                b_eq = np.array([1])
                bnds = [(low_bound_qty, up_bound_qty)
                        for i in range(len(basket['syms']))]
                bnds.append((None, None))
                res = op.linprog(target_func, A_ub, b_ub, A_eq,
                                 b_eq, bounds=bnds, method='interior-point')

                res_i = 0
                for sym, value in data_dict.items():
                    data_dict[sym]['target_qty'] = res.x[res_i]
                    data_dict[sym]['target_side'] = target_side
                    data_dict[sym]['diff'] = res.x[res_i] * \
                        target_side - value['qty']*value['side']
                    res_i += 1

                context.logger.info(f"time takes {datetime.now()-start}")
                #context.logger.info(f"basket_bs_gamma is {basket_bs_gamma},current_hedge_gamma is {current_hedge_gamma},basket_bs_vega is {basket_bs_vega},current_hedge_vega is {current_hedge_vega}")
                context.logger.info(
                    f"group_gamma is {group_gamma},group_vega is {group_vega},data_dict:{data_dict}")
                context.logger.info(
                    f"gamma is hedged to {abs(group_gamma)-np.dot(res.x[:-1],gs)},vega is hedged to {abs(group_vega)-np.dot(res.x[:-1],vs*10000)}")

                if context.auto_hedge and warm_up_time > 0.5:
                    if abs(group_gamma) > context.gamma_maker_limit or abs(group_vega) > context.vega_maker_limit:
                        context.logger.info("in maker limit beach")
                        context.hedge_execution(
                            group, group_gamma, group_vega, data_dict)
            except:
                context.logger.error(traceback.format_exc())
                context.logger.error(f'EXCEPTION:hedge_group_opiton_df:{hedge_group_opiton_df}')

    user_data = {
        'type': 'gamma',
        f'{currency[:3]}':{
        'account_gamma': '%.6f' % account_gamma,
        'account_vega': '%.4f' % account_vega,
        'exposure_gamma': '%.6f' % exposure_gamma,
        'exposure_vega': '%.4f' % exposure_vega,
        'deribit_gamma': '%.6f' % deribit_gamma,
        'deribit_vega': '%.4f' % deribit_vega,
        }
    }
    context.send_data_to_user(user_data)


def hedge_execution(context, group, group_gamma, group_vega, data_dict):
    with context.lock:
        if group in context.sent_orders_dict:
            cur_syms = set(context.sent_orders_dict[group])
        else:
            context.sent_orders_dict[group] = {}
            cur_syms = set()
        new_syms = set(data_dict.keys())
        in_syms = cur_syms & new_syms  # 交集, 进一步判断需不需要modify order
        add_syms = new_syms - cur_syms  # 在new_syms中，不在cur_syms中，需要新发的orders
        out_syms = cur_syms - new_syms  # 在cur_syms中，不在new_syms中，需要撤销的orders
        context.new_syms[group] = {sym: data_dict[sym] for sym in add_syms}
        context.cancel_syms[group] = {sym: True for sym in out_syms}
        
        #Handle send orders in add_syms, if no out_syms orders needed to be canncelled, send order.
        context.logger.info(f'Preparing new orders,add_syms:{add_syms},context.sent_orders_dict:{context.sent_orders_dict},context.orders_existed:{context.orders_existed}')
        for sym in add_syms:
            if not sym in context.orders_existed:
                aggresive = 'maker'
                spread = data_dict[sym]['best_ask'] - data_dict[sym]['best_bid']
                if spread <= context.taker_spread*context.tick_size and (abs(group_gamma) > context.gamma_taker_limit or abs(group_vega) > context.vega_taker_limit):
                    aggresive = 'taker'
                context.new_syms[group][sym]['aggresive'] = aggresive
                if len(out_syms) == 0:
                    note = {'aggresive': aggresive, 'group': group,
                        'target_qty': data_dict[sym]['target_qty'], 'target_side': data_dict[sym]['target_side']}
                    context.send_limit_order(sym, data_dict[sym], note)
        
        #Handle cancel orders in out_syms
        context.logger.info(f'Cancelling orders,out_syms:{out_syms},context.sent_orders_dict:{context.sent_orders_dict}')
        for sym in out_syms:
            sent_order_id = context.sent_orders_dict[group][sym]
            open_order = context.orders[sent_order_id]
            note = {'send_new': True, 'group': group,
                    'target_qty': open_order['note']['target_qty'], 'target_side': open_order['note']['target_side']}
            context.cancel_order(f'Deribit|{sym}|option', sent_order_id, note=note)

        for sym in in_syms:
            sent_order_id = context.sent_orders_dict[group][sym]
            open_order = context.orders[sent_order_id]
            note = {'send_new': False, 'group': group,
                    'target_qty': data_dict[sym]['target_qty'], 'target_side': data_dict[sym]['target_side']}
            context.logger.info(f'open_order is {open_order}, market data is {data_dict[sym]}')
            if open_order['note']['aggresive'] == 'maker':
                order_side = open_order['direction']
                diff = data_dict[sym]['diff']
                if diff >= 0:
                    side = Direction.Buy
                    best_price = data_dict[sym]['best_bid']
                elif diff < 0:
                    side = Direction.Sell
                    best_price = data_dict[sym]['best_ask']

                context.logger.info(
                    f'inside order_excution, there is an exsting order, side:{side}, order_side:{order_side}, best_price:{best_price},open_price:{open_order["price"]}')
                if side.lower() != order_side.lower():
                    context.cancel_order(
                        f'Deribit|{sym}|option', sent_order_id, note=note)
                elif best_price != open_order['price']:
                    note['aggresive'] = 'maker'
                    context.modify_order(
                        f'Deribit|{sym}|option', sent_order_id, best_price, abs(diff), note=note)
            else:
                context.cancel_order(
                    f'Deribit|{sym}|option', sent_order_id, note=note)
