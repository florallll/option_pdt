from utils.util_func import get_human_readable_timestamp
import numpy as np
import pandas as pd
import math
import pickle
import traceback
from utils.util_func import *
from datetime import datetime
from utils.SABRLib import *
from utils.OptionUtil import *
from utils.intercom import Intercom
from configs.typedef import IntercomScope, IntercomChannel, TradeSide, OrderType, Direction, OrderStatus


template = "together_template"

# pre_start：交易（回测）开始前运行


def pre_start(context, **kwargs):
    context.logger.info(f'DemoStrategy::run before trading start: {kwargs}')
    context.hedge_time = datetime.now()
    context.account_target = 100
    context.positions = kwargs['positions']
    #context.positions = get_agg_positions_set_ccy(positions_df)
    context.hedge_positions = {'BTCUSD': {}, 'ETHUSD': {}}
    context.currency = kwargs['currency']
    context.delta_time_limit = kwargs['delta_time_limit']
    context.delta_limit_taker = kwargs['delta_limit_taker']
    context.delta_limit_maker = kwargs['delta_limit_maker']
    context.intercom = Intercom()
    #context.set_interval(5, context.hedge)
    context.set_interval(3, context.hedge_b2b)
    #context.set_interval(5, context.self_check)


def on_market_data_ticker_ready(context, data):
    pass
    #context.logger.info(f'inside ticker {data}')
    # key = data['symbol']
    # if 'instrument_id' in data:
    #     key = data['instrument_id']
    # context.symbol_dict[key] = data['metadata']
    # context.symbol_dict[key]['timestamp'] = data['timestamp']


def on_market_data_summaryinfo_ready(context, message):
    pass


def on_response_inspect_order_ready(context, data):
    context.logger.info('inside inspect order')
    context.logger.info(data)
    error = "error_code" in data['metadata']['metadata']
    with context.lock:
        if not error:
            context.update_order(data['metadata'])
            #order_id = data['metadata']['metadata']['order_id']
            # if not (context.orders[order_id]['status'] == OrderStatus.Filled or context.orders[order_id]['status'] == OrderStatus.Cancelled):
            # context.update_position(data['metadata'])
        else:
            context.logger.info(
                f"inspect order error {data['metadata']['metadata']['error_code_msg']}")


def on_response_cancel_order_ready(context, data):
    context.logger.info("inside cancel order")
    context.logger.info(data)
    symbol_df = pd.DataFrame(context.symbol_dict).T
    future_data = symbol_df.loc[context.currency]
    metadata = data['metadata']['request']['metadata']
    error = "error_code" in data['metadata']['metadata']
    with context.lock:
        if not error:
            context.update_order(data['metadata'])
            delta_diff = metadata['delta_diff']
            context.order_id = None
            #context.logger.info(f'resent:{future_data}, delta_diff:{delta_diff}')
            context.send_limit_order(future_data, delta_diff, metadata['note'])
        else:
            context.logger.info(
                f"cancel order error:{data['metadata']['metadata']['error_code_msg']}")
            exchange = metadata['exchange']
            symbol = metadata['symbol'] if metadata['instrument_id'] == "" else metadata['instrument_id']
            contract_type = metadata['contract_type']
            order_id = metadata['order_id']
            instrument = '|'.join([exchange, symbol, contract_type])
            context.inspect_order(instrument, order_id, note=metadata['note'])


def on_position_booking(context, data):
    context.logger.info(f"new position booked {data}")
    if data['currency'] == context.currency:
        params = ['symbol','qty','customer','side','settlement_ccy','avg_price','b2b_instrument','listed','b2b_bid']
        instrument,qty,booking_id,side,settlement_ccy,avg_price,b2b_instrument,listed,b2b_bid = [data[_] for _ in params]

        context.positions[booking_id] = {
            'symbol': instrument,
            'avg_price': avg_price,
            'qty': qty,
            'b2b_instrument': b2b_instrument,
            'listed': listed
        }
        if b2b_instrument:
            currency, exp_date, strike, cp = instrument.split("-")
            if currency == "BTCUSD":
                vol_SABR = context.BTC_vol_SABR
            else:
                vol_SABR = context.ETH_vol_SABR

            bitYield = vol_SABR['yieldCurve']['rate']
            ttm = vol_SABR['yieldCurve']['tenor']
            fwd = vol_SABR['fwdCurve']['rate'][0]
            exp = datetime.strptime("{}".format(exp_date+" 16:00:00"), "%Y%m%d %H:%M:%S")
            dtm = ((exp-datetime.now()).total_seconds()/3600/24)/365
            vol = get_vol(vol_SABR, dtm, float(strike))
            fwdYield = interp(dtm, ttm, bitYield)
            fwd_price = fwd*math.exp(fwdYield*dtm)
            eu_option = BSmodel(float(strike), py2ql_date(exp), cp, 'forward')
            eu_option.price(py2ql_date(datetime.now()), fwd_price, vol, 0)
            view = eu_option.view()
            view['iv'] = vol*100
            view['price'] = view['price']/fwd_price
            context.positions[booking_id].update(view)
            target = qty if listed else qty*avg_price
            time_interval = (0.5 + dtm*365/180*(30-10))*60
            context.hedge_b2b_dict[booking_id] = {
                'booking_id': booking_id,
                'b2b_instrument' : b2b_instrument,
                'target':target,
                'b2b_bid':b2b_bid,
                'listed':listed,
                'time_interval' : time_interval,
                'finished':False
            }       
            best_bid, bid_amount = get_best_bid(b2b_instrument)
            if best_bid!=0:
                price = best_bid
            else:
                best_ask,ask_amount = get_best_ask(b2b_instrument)
                if best_ask!=0:
                    price = best_bid - context.min_tick
                else:
                    price = get_mark_price(b2b_instrument)                    
            target_qty = qty if listed else target/price
            amount = bid_amount if bid_amount else target_qty

            sum_bids = get_sum_bids(b2b_instrument)
            size_ratio = target_qty/sum_bids if sum_bids else 1
            size_perc = 0.2 if size_ratio < 0.2 else max(1,size_ratio)
            vol_perc = 1 if b2b_bid/price > 0.99 else size_perc
            OrderQty = max(min(target_qty,amount*vol_perc),context.lot_size)

            context.positions[booking_id].update({'b2b_qty':0,'cum_qty':OrderQty})
            context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
            note = {'is_basket': False, 'booking_id': booking_id}
            dt = max(int(np.ceil(OrderQty/target_qty*time_interval)),3)*1000 if vol_perc < 1 and OrderQty/target_qty <= 0.5 else 3000
            context.logger.info(f"OrderQty: {OrderQty},bid_amount:{bid_amount},price:{price},  time_interval:{time_interval},dt: {dt/1000}")
            context.send_order(f'Deribit|{b2b_instrument}|option', TradeSide.Sell, price, OrderQty, OrderType.Fak, delay = dt, note = note)
        context.logger.info(pd.DataFrame(context.positions).T)

def on_auto_hedge(context, data):
    pass


def on_target_update(context, data):
    pass

    # with context.lock:
    #     target,currency = data
    #     if currency == context.currency[:3]:
    #         context.logger.info(f'DemoStrategy::on_target_update received:{data}')
    #         context.account_target = float(target)
    #         context.send_data_to_user({"type": "target_update", "target": target})


def on_signal_ready(context, data):
    context.logger.info(f'DemoStrategy::signal received')
    with context.lock:
        try:
            data['volDate'] = datetime.strptime(data['volDate'], "%Y%m%d")
            if data['currency'] == "BTC":
                context.BTC_vol_SABR = data
            else:
                context.ETH_vol_SABR = data
            bitYield = data['yieldCurve']['rate']
            ttm = data['yieldCurve']['tenor']
            fwd = data['fwdCurve']['rate'][0]

            for key,values in context.positions.items():
                currency, exp, strike, cp = values['symbol'].split('-')
                if currency.find(data['currency']) != -1:
                    exp = datetime.strptime("{}".format(exp+" 16:00:00"), "%Y%m%d %H:%M:%S")
                    dtm = ((exp-datetime.now()).days +(exp-datetime.now()).seconds/3600/24)/365
                    vol = get_vol(data, dtm, float(strike))
                    fwdYield = interp(dtm, ttm, bitYield)
                    fwd_price = fwd*math.exp(fwdYield*dtm)

                    view = {}
                    eu_option = BS([fwd_price, float(strike), 0,dtm*365], volatility=vol*100)
                    view['iv'] = vol*100
                    price = eu_option.callPrice if cp == "C" else eu_option.putPrice
                    view['price'] = round(price/fwd, 4)
                    delta = eu_option.callDelta if cp == "C" else eu_option.putDelta
                    view['delta'] = round(delta, 4)
                    theta = eu_option.callTheta if cp == "C" else eu_option.putTheta
                    view['theta'] = round(theta, 4)
                    view['gamma'] = round(eu_option.gamma, 4)
                    view['vega'] = round(eu_option.vega, 4)

                    context.positions[key].update(view)

            for base in context.hedge_positions:
                #hedge_df = pd.DataFrame(context.hedge_positions[base]).T
                future_sym = base+"_perp"
                hedged_options = {
                    key: value for key, value in context.hedge_positions[base].items() if key != future_sym}
                for key in hedged_options:
                    currency, exp, strike, cp = key.split('_')[0].split('-')
                    if currency.find(data['currency']) != -1:
                        exp = datetime.strptime("{}".format(exp+" 16:00:00"), "%Y%m%d %H:%M:%S")
                        dtm = ((exp-datetime.now()).days +(exp-datetime.now()).seconds/3600/24)/365
                        vol = get_vol(data, dtm, float(strike))
                        fwdYield = interp(dtm, ttm, bitYield)
                        fwd_price = fwd*math.exp(fwdYield*dtm)

                        view = {}
                        eu_option = BS([fwd_price, float(strike), 0,
                                        dtm*365], volatility=vol*100)
                        view['iv'] = vol*100
                        price = eu_option.callPrice if cp == "C" else eu_option.putPrice
                        view['price'] = round(price/fwd, 4)
                        delta = eu_option.callDelta if cp == "C" else eu_option.putDelta
                        view['delta'] = round(delta, 4)
                        theta = eu_option.callTheta if cp == "C" else eu_option.putTheta
                        view['theta'] = round(theta, 4)
                        view['gamma'] = round(eu_option.gamma, 4)
                        view['vega'] = round(eu_option.vega, 4)
                        context.hedge_positions[base][key].update(view)
        except:
            context.logger.error(traceback.format_exc())
            #context.logger.info(f"Error - volData is:{data}")
    

# on_response_{action}_ready: 处理response的回调函数
def on_response_place_order_ready(context, data):
    context.logger.info(f'DemoStrategy::send order response received')
    context.logger.info(f'in place order {data}')
    metadata = data['metadata']
    error = "error_code" in metadata['metadata']
    note = metadata['request']['metadata']['note']
    currency = metadata['symbol']
    if not error:
        with context.lock:
            if not note['is_basket']:
                booking_id = note['booking_id']
                context.logger.info(f'Parent order:{context.hedge_b2b_dict[booking_id]}')
                b2b_instrument = context.hedge_b2b_dict[booking_id]['b2b_instrument']
                listed = context.hedge_b2b_dict[booking_id]['listed']
                delay = metadata['request']['metadata']['delay']
                quantity = metadata['request']['metadata']['quantity']
                filled_quantity = metadata['request']['metadata']['filled_quantity']
                avg_executed_price = metadata['request']['metadata']['avg_executed_price']
                context.positions[booking_id]['b2b_qty'] += filled_quantity
                context.positions[booking_id]['cum_qty'] -= (quantity - filled_quantity)
                context.hedge_b2b_dict[booking_id]['target'] -= filled_quantity if listed else filled_quantity*avg_executed_price
                context.hedge_b2b_dict[booking_id]['time_interval'] -= filled_quantity/quantity*delay/1000
                best_bid, bid_amount = get_best_bid(b2b_instrument)
                if best_bid!=0:
                    price = best_bid
                    amount = bid_amount
                else:
                    best_ask,ask_amount = get_best_ask(b2b_instrument)
                    if best_ask!=0:
                        price = best_bid - context.min_tick
                    else:
                        price = get_mark_price(b2b_instrument)
                    amount = context.hedge_b2b_dict[booking_id]['target'] if listed else context.hedge_b2b_dict[booking_id]['target']/price

                target_qty = context.hedge_b2b_dict[booking_id]['target'] if listed else context.hedge_b2b_dict[booking_id]['target']/price
                if target_qty < context.lot_size:
                    context.hedge_b2b_dict[booking_id]['finished'] = True
                    context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                    #context.positions[booking_id]['b2b_qty'] = context.positions[booking_id]['cum_qty']
                    #del context.positions[booking_id]['cum_qty']
                    del context.hedge_b2b_dict[booking_id]
                    return

                sum_bids = get_sum_bids(b2b_instrument)
                size_ratio = target_qty/sum_bids if sum_bids else 1
                size_perc = 0.2 if size_ratio < 0.2 else max(1,size_ratio)
                vol_perc = 1 if context.hedge_b2b_dict[booking_id]['b2b_bid']/price > 0.99 else size_perc
                OrderQty = max(min(target_qty,amount*vol_perc),context.lot_size)

                dt = max(int(np.ceil(OrderQty/target_qty*context.hedge_b2b_dict[booking_id]['time_interval'])),3)*1000 if vol_perc < 1 and OrderQty/target_qty <= 0.5 else 3000
                context.send_order(f'Deribit|{b2b_instrument}|option', TradeSide.Sell, price, OrderQty, OrderType.Fak, delay = dt, note = note)
                context.logger.info(f"booking_id:{booking_id},target:{context.positions[booking_id]['qty']},remaining:{context.hedge_b2b_dict[booking_id]['target']},this time sent:{OrderQty},time_interval:{dt},b2b_bid:{context.hedge_b2b_dict[booking_id]['b2b_bid']},best_bid:{best_bid}")
                context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                context.positions[booking_id]['cum_qty'] += OrderQty

            else:
                context.order_existed = False
                symbol = metadata['symbol'] if metadata['request']['metadata'][
                    'instrument_id'] == "" else metadata['request']['metadata']['instrument_id']
                key = symbol+'_perp'
                #context.local_hedge_info[key] = {'group':'perp', 'greek':'delta'}
                context.update_order(metadata)
    else:
        context.logger.error(f"place order error:{metadata['metadata']['error_code_msg']}")

def on_order_ready(context, data):
    pass


# on_position_ready: 处理position_info的回调函数
def on_position_ready(context, data):
    #context.logger.info(f'inside postion ready {data}')
    posInfoType = data['metadata']['posInfoType']
    if posInfoType == "future_position" or posInfoType == "option_position":
        hedge_positions = data['metadata']['metadata']
        for col in context.position_del_cols:
            del hedge_positions[col]

        currency = data['metadata']['symbol']
        future_sym = currency+"_perp"
        if posInfoType == "future_position" and future_sym not in hedge_positions and future_sym in context.hedge_positions[currency]:
            del context.hedge_positions[currency][future_sym]
        elif posInfoType == "option_position":
            hedged_options = {key: value for key, value in context.hedge_positions[currency].items() if key != future_sym}
            cleared_positions = [key for key in hedged_options if key not in hedge_positions]
            for key in cleared_positions:
                del context.hedge_positions[currency][key]

        for key, value in hedge_positions.items():
            position = {}
            if hedge_positions[key]['buy_available'] > 0:
                position['side'] = 1
                position['qty'] = hedge_positions[key]['buy_available']
            else:
                position['side'] = -1
                position['qty'] = hedge_positions[key]['sell_available']

            if key in context.hedge_positions[currency]:
                context.hedge_positions[currency][key].update(position)
            else:
                context.hedge_positions[currency][key] = position

        if 'future_userinfo' in data['global_balances']:
            if context.currency == "BTCUSD":
                context.account_equity = data['global_balances']['future_userinfo']['BTC_rights']
                context.option_value = data['global_balances']['future_userinfo']['BTCUSD_option_value']
            else:
                context.account_equity = data['global_balances']['future_userinfo']['ETH_rights']
                context.option_value = data['global_balances']['future_userinfo']['ETHUSD_option_value']



def update_order(context, data):
    metadata = data['request']['metadata']
    context.logger.info(f'inside order upate:{data}')

    if 'options' in metadata:
        del metadata['options']
    order_id = metadata['order_id']

    if order_id in context.orders:
        context.orders[order_id].update(metadata)
        side = 1 if context.orders[order_id]['direction'].lower() == 'buy' else -1
        quantity = context.orders[order_id]['quantity']
        symbol = context.orders[order_id]['symbol']
    else:
        context.orders[order_id] = metadata
        side = 1 if metadata['direction'].lower() == 'buy' else -1
        quantity = metadata['quantity']
        symbol = metadata['symbol']

    status = metadata['status']
    if status == OrderStatus.Filled or status == OrderStatus.Cancelled:
        key = symbol + "_perp"
        target_qty = metadata['note']['original_qty'] * \
            metadata['note']['original_side'] + side*quantity
        target_side = 1 if target_qty > 0 else -1
        if key in context.hedge_positions[symbol]:
            context.hedge_positions[symbol][key]['qty'] = abs(target_qty)
            context.hedge_positions[symbol][key]['side'] = target_side
        else:
            context.hedge_positions[symbol][key] = {'qty': abs(target_qty), 'side': target_side}

        context.sent_orderId = None
    else:
        context.sent_orderId = order_id


# 其它
# context.set_interval可设定周期性运行的函数
# context.register_handler可以将回调函数注册至指定redis channel
# 待新增

def on_flow_update(context, data):
    pass

def check_open_order(context):
    context.logger.info(f'check_open_order:{datetime.now()}')
    
    with context.lock:
        open_orders = {key: value for key, value in context.orders.items() if value['status'] == OrderStatus.Submitted
                        or value['status'] == OrderStatus.PartiallyFilled or value['status'] == OrderStatus.Unknown}
        for key, value in open_orders.items():
            exchange = value['exchange']
            symbol = value['symbol'] if value['instrument_id'] == "" else value['instrument_id']
            contract_type = value['contract_type']
            instrument = '|'.join([exchange, symbol, contract_type])
            context.inspect_order(instrument, key, note=value['note'])

        # remove any expired positions
        for key,value in context.positions.items():
            currency, expiry, strike, cp = value['symbol'].split('-')
            exp = datetime.strptime("{}".format(expiry+" 16:00:00"), "%Y%m%d %H:%M:%S")
            dtm = ((exp-datetime.now()).days +(exp-datetime.now()).seconds/3600/24)
            if dtm <= 0:
                del context.positions[key]

def hedge_b2b(context):
    with context.lock:
        try:
            '''
            listed_options = {key: value for key, value in context.positions.items() if not value['hedged'] and value['listed']}
            unlisted_options = {key: value for key, value in context.positions.items() if not value['hedged'] and not value['listed']}

            
            for key,value in listed_options.items():
                if not key in sent_orders_dict:
                    symbol = value['b2b_instrument']
                    sym_tick = context.symbol_dict[symbol]
                    value['target_qty'] = value['qty']
                    value['coid'] = key
                    #TODO: send order by VWAP
                    context.send_twap_order(value, sym_tick)
            
            for key,value in unlisted_options.items():
                if not key in sent_orders_dict:
                    symbol = value['b2b_instrument']
                    sym_tick = context.symbol_dict[symbol]
                    target_vega = value['vega'] * value['qty']
                    target_qty = target_vega / sym_tick['vega']
                    value['target_qty'] = target_qty
                    value['coid'] = key
                    #TODO: send order by VWAP
                    context.send_twap_order(value, sym_tick)
            '''
            #context.logger.info(f'listed_options:{listed_options}, unlisted options:{unlisted_options}')
        except Exception as e:
            context.logger.error(e)


def hedge(context):
    with context.lock:
        context.logger.info(f'hedge_delta:{datetime.now()}')
        time_diff = datetime.now() - context.hedge_time
        minutes_diff = time_diff.days * 1440 + time_diff.seconds/60
        from_start_to_now = datetime.now() - context.strgy_start_time
        warm_up_time = from_start_to_now.days * 1440 + from_start_to_now.seconds/60
        try:
            position_df = pd.DataFrame(context.positions).T
            #context.logger.info(f'context.account_equity:{context.account_equity}, context.symbol_dict:{context.symbol_dict}')

            if context.account_equity != 0 and ("delta" in position_df or len(position_df) == 0) and context.currency in context.symbol_dict:
                future_data = context.symbol_dict[context.currency]
                #context.logger.info(f'inside hedge delta:{context.hedge_positions["BTCUSD"]}')
                if f'{context.currency}_perp' in context.hedge_positions[context.currency]:
                    future_position = context.hedge_positions[context.currency][f'{context.currency}_perp']
                    future_qty = future_position['qty']
                    future_side = future_position['side']
                    contract_size = 10 if context.currency == 'BTCUSD' else 1
                    future_delta = future_qty * contract_size * future_position['side']/future_data['mark_price']
                else:
                    future_qty = 0
                    future_side = 0
                    future_delta = 0

                hedged_options = {key: value for key, value in context.hedge_positions[context.currency].items(
                ) if key != f'{context.currency}_perp'}
                hedged_options_df = pd.DataFrame(hedged_options).T
                if len(hedged_options_df) > 0 and "delta" in hedged_options_df:
                    hedged_options_delta = (
                        hedged_options_df['delta'] * hedged_options_df['qty'] * hedged_options_df['side']).sum()
                else:
                    hedged_options_delta = 0
                #options_index = [idx for idx in symbol_df.index if idx != 'BTCUSD']
                #option_df = symbol_df.loc[options_index]

                if len(position_df) == 0:
                    usd_option_delta = 0
                    cryto_option_delta = 0
                else:
                    cryto_settle_options = position_df[position_df['settlement_ccy'] != 'USD']
                    usd_settle_options = position_df[position_df['settlement_ccy'] == 'USD']
                    # 因为我们是客户的对手方所以需要乘以-1
                    usd_option_delta = -1 * \
                        (usd_settle_options['net_qty'] *
                         usd_settle_options['delta']).sum()
                    cryto_option_bs_delta = -1 * \
                        (cryto_settle_options['net_qty'] *
                         cryto_settle_options['delta']).sum()
                    cryto_option_premium = -1 * \
                        (cryto_settle_options['net_qty'] *
                         cryto_settle_options['price']).sum()
                    cryto_option_delta = cryto_option_bs_delta - cryto_option_premium

                hedged_options_bs_delta = hedged_options_delta - context.option_value
                customer_options_delta = usd_option_delta + cryto_option_delta
                total_option_delta = hedged_options_bs_delta + customer_options_delta
                current_account_delta = context.account_equity + total_option_delta + future_delta
                delta_diff = context.account_target - current_account_delta
                context.logger.info(
                    f"time diff is {minutes_diff},current_account_delta is {current_account_delta}, delta total is {round(delta_diff,4)}")

                if context.auto_hedge and type(current_account_delta) != complex and warm_up_time > 0.5:
                    if minutes_diff > context.delta_time_limit:
                        context.logger.info("in minutes_diff")
                        note = {
                            'aggresive': "maker", "original_qty": future_qty, "original_side": future_side}
                        context.order_execution(future_data, delta_diff, note)
                        context.hedge_time = datetime.now()
                        delta_data = {'hedge_time': context.hedge_time,
                                      'account_target': context.account_target}
                        with open(f'data/{context.currency[:3]}_delta_data.pkl', 'wb') as fw:
                            pickle.dump(delta_data, fw)
                    elif abs(delta_diff) > context.delta_limit_maker and abs(delta_diff) < context.delta_limit_taker:
                        context.logger.info("in limit_maker")
                        note = {
                            'aggresive': "maker", "original_qty": future_qty, "original_side": future_side}
                        context.order_execution(future_data, delta_diff, note)
                    elif abs(delta_diff) > context.delta_limit_taker:
                        context.logger.info("in limit_taker")
                        note = {
                            'aggresive': "taker", "original_qty": future_qty, "original_side": future_side}
                        context.order_execution(future_data, delta_diff, note)
                    elif not context.sent_orderId is None:
                        context.logger.info("in exsiting order")
                        note = {
                            'aggresive': "maker", "original_qty": future_qty, "original_side": future_side}
                        context.order_execution(future_data, delta_diff, note)

                account_data = {
                    'type': 'delta',
                    f'{context.currency[:3]}':{
                            'target': '%.4f' % context.account_target,
                            'equity': '%.4f' % context.account_equity,
                            'hedged_options_delta': '%.4f' % hedged_options_bs_delta,
                            'customer_options_delta': '%.4f' % customer_options_delta,
                            'account_option_delta': '%.4f' % total_option_delta,
                            'future_delta': '%.4f' % future_delta,
                            'account_delta': '%.4f' % (-delta_diff),
                            'auto_hedge': context.auto_hedge
                            }
                }
                context.send_data_to_user(account_data)

                if len(position_df) != 0:
                    positions_data = {
                        'type': 'user_positions',
                        f'{context.currency[:3]}':{
                        'quotes': context.positions,
                        'hedged_positoins': context.hedge_positions[context.currency]
                        }
                    }                    
                    context.send_data_to_user(positions_data)
        except:
            context.logger.error(traceback.format_exc())


def order_execution(context, market_data, delta_diff, note):
    if delta_diff == 0 or context.order_existed:
        return
    # If there is an existing order:
    # If it's a taker order, cancel it directly
    # If it's a maker order, cancel it if the price does not match the best price or the side is changed
    context.logger.info(f'sent order id is {context.sent_orderId}')
    if not context.sent_orderId is None:
        open_order = context.orders[context.sent_orderId]
        context.logger.info(f'open_order is {open_order}')
        if open_order['note']['aggresive'] == 'maker':
            order_side = open_order['direction']
            if delta_diff > 0:
                side = Direction.Buy
                best_price = market_data['best_bid']
            elif delta_diff < 0:
                side = Direction.Sell
                best_price = market_data['best_ask']

            context.logger.info(
                f'inside order_excution, there is an exsting order, side:{side}, order_side:{order_side}, best_price:{best_price},open_price:{open_order["price"]}')
            if side.lower() != order_side.lower() or best_price != open_order['price']:
                context.cancel_order(
                    f'Deribit|{context.currency}|perp', context.sent_orderId, delta_diff=delta_diff, note=note)
        else:
            context.cancel_order(
                f'Deribit|{context.currency}|perp', context.sent_orderId, delta_diff=delta_diff, note=note)
    else:
        # If there has no existing order, send order to the market
        context.send_limit_order(market_data, delta_diff, note)

def self_check(context):
    pass
