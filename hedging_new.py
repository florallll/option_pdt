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


template = "hedging_template"

# pre_start：交易（回测）开始前运行

def pre_start(context, **kwargs):
    context.logger.info(f'DemoStrategy::run before trading start: {kwargs}')
    context.account_target = 100
    context.positions = kwargs['positions']
    context.hedge_positions = {'BTCUSD': {}, 'ETHUSD': {}}
    context.currency = kwargs['currency']
    context.future_delta = kwargs['future_delta']
    context.delta_time_limit_basket2 = kwargs['delta_time_limit']
    context.delta_limit_taker_basket2 = kwargs['delta_limit_taker']
    context.delta_limit_maker_basket2 = kwargs['delta_limit_maker']
    context.delta_time_limit_basket3 = kwargs['delta_time_limit']*1.5
    context.delta_limit_taker_basket3  = kwargs['delta_limit_taker']*1.5
    context.delta_limit_maker_basket3  = kwargs['delta_limit_maker']*1.5
    context.intercom = Intercom()
    context.time_limit = kwargs['time_limit']
    context.gamma_maker_limit = kwargs['gamma_maker_limit']
    context.gamma_taker_limit = kwargs['gamma_taker_limit']
    context.vega_maker_limit = kwargs['vega_maker_limit']
    context.vega_taker_limit = kwargs['vega_taker_limit']
    context.taker_spread = kwargs['taker_spread']
    context.max_qty = kwargs['max_qty']
    context.set_interval(2, context.check_open_order)
    context.set_interval(10, context.basket2_vega_hedge)
    context.set_interval(5, context.basket2_delta_hedge)
    context.set_interval(10, context.basket3_gamma_hedge)
    context.set_interval(20, context.basket3_delta_hedge)


def on_market_data_ticker_ready(context, data):
    pass


def on_market_data_1min_ready(context, data):
    pass


def on_market_data_trade_ready(context, data):
    context.logger.info("inside marketdata trade ready")


def on_market_data_summaryinfo_ready(context, message):
    pass


def on_response_inspect_order_ready(context, data):
    context.logger.info('inside inspect order{data}')
    error = "error_code" in data['metadata']['metadata']
    with context.lock:
        if not error:
            context.update_order(data['metadata'])
        else:
            context.logger.info(f"inspect order error {data['metadata']['metadata']['error_code_msg']}")


def on_response_cancel_order_ready(context, data):
    context.logger.info("inside cancel order {data}")
    metadata = data['metadata']['request']['metadata']
    error = "error_code" in data['metadata']['metadata']
    note = metadata['note']
    with context.lock:
        if 'action' in note:
            #! only listed order would be chanceled and revert to new routine
            b2b_instrument = metadata['instrument_id']
            booking_id = note['booking_id']
            #! cancel successful = not successful but filled
            if not error or data['metadata']['metadata']['error_code_msg'] == 'not open order':
                # if booking_id not in context.hedge_b2b_dict:
                #     return 
                this_b2b_dict = context.hedge_b2b_dict[booking_id]
                b2b_bid_now = price_filter_by_volume(b2b_instrument, avg = True)
                target_qty = this_b2b_dict['target'] 
                if not b2b_bid_now:
                    b2b_ask_now = get_best_ask(b2b_instrument)[0]
                    if not b2b_ask_now:
                        b2b_price_now = get_mark_price(b2b_instrument)
                    else:
                        b2b_price_now = b2b_ask_now - context.min_tick
                else:
                    b2b_price_now = b2b_bid_now 

                need_to_move = False

                if note['action'] == 'monitor_order':
                    if 0.95 < this_b2b_dict['b2b_bid']/b2b_price_now <= 0.995:
                        #! this would contine fast b2b hedge routine
                        b2b_price_now = price_filter_by_volume(b2b_instrument, exclude_volume = target_qty, avg = False)
                        if target_qty >= context.lot_size:
                            context.send_order(f"Deribit|{b2b_instrument}|option", TradeSide.Sell, b2b_price_now, target_qty, OrderType.Fak, note = note)
                        else:
                            context.hedge_b2b_dict[booking_id]['finished'] = True
                            context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                            if not context.hedge_b2b_dict[booking_id]['listed']:
                                context.positions[booking_id]['basket1'] = False
                            del context.hedge_b2b_dict[booking_id]
                    elif 0.995 < this_b2b_dict['b2b_bid']/b2b_price_now:
                        #!this would move to 2nd basket and stop b2b hedge
                        need_to_move = True
                    else:
                        #! this would revert back to slow b2b hedge routine
                        best_bid, bid_amount = get_best_bid(b2b_instrument)
                        del note['action']
                        if best_bid:
                            b2b_price_now = best_bid
                            target_qty = min(this_b2b_dict['target'] if this_b2b_dict['listed'] else this_b2b_dict['target']/best_bid, bid_amount)
                            # if target_qty >= context.lot_size:
                            #     context.send_order(f"Deribit|{b2b_instrument}|option", TradeSide.Sell, best_bid, bid_amount, OrderType.Fak, delay = 3000, note = note)
                            # else:
                            #     context.hedge_b2b_dict[booking_id]['finished'] = True
                            #     context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                            #     del context.hedge_b2b_dict[booking_id]
                        
                        if target_qty >= context.lot_size:
                            context.send_order(f"Deribit|{b2b_instrument}|option", TradeSide.Sell, b2b_price_now, target_qty, OrderType.Limit, note = note)
                        else:
                            context.hedge_b2b_dict[booking_id]['finished'] = True
                            context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                            if not context.hedge_b2b_dict[booking_id]['listed']:
                                context.positions[booking_id]['basket1'] = False
                            del context.hedge_b2b_dict[booking_id]

                # elif metadata['monitor'] == 'second_threshold' or need_to_move:
                if note['action'] == 'move_to_basket' or need_to_move:
                    context.hedge_b2b_dict[booking_id]['finished'] = True
                    context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                    del context.hedge_b2b_dict[booking_id]
                    context.positions[booking_id]['basket1'] = False
                    
            # elif data['metadata']['metadata']['error_code_msg'] == 'not open order':
            #     if booking_id not in context.hedge_b2b_dict
            #     #!cancel not successful meaning order filled on the way
            #     context.logger.error(f"cancel order error:{data['metadata']['metadata']['error_code_msg']}")


            else:
                context.logger.error(f"cancel order error:{data['metadata']['metadata']['error_code_msg']}")
        else:
            if not error:
                if metadata['contract_type'] == 'future':
                    future_data = get_instrument_summaryinfo(context.currency)
                    context.update_order(data['metadata'])
                    delta_diff = metadata['delta_diff']
                    context.order_id[metadata['note']['basket']] = None
                    #context.logger.info(f'resent:{future_data}, delta_diff:{delta_diff}')
                    context.send_limit_order(market_data = future_data, delta_diff = delta_diff, note = metadata['note'])
                else:
                    instrument_id = metadata['instrument_id']
                    market_data = get_instrument_summaryinfo(instrument_id)
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
                                note = {'aggresive': aggresive, 'group': group, 
                                        'target_qty': context.new_syms[group][sym]['target_qty'],
                                        'target_side': context.new_syms[group][sym]['target_side']}
                                context.send_limit_order(symbol = sym, market_data = market_data, note = note)
                                context.logger.info(f"[SENT NEW SYM] {sym}:{note}")

            else:
                context.logger.error(f"cancel order error:{data['metadata']['metadata']['error_code_msg']}")
                exchange = metadata['exchange']
                symbol = metadata['symbol'] if metadata['instrument_id'] == "" else metadata['instrument_id']
                contract_type = metadata['contract_type']
                order_id = metadata['order_id']
                instrument = '|'.join([exchange, symbol, contract_type])
                context.inspect_order(instrument, order_id, note=metadata['note'])


def on_position_booking(context, data):
    context.logger.info(f"new position booked {data}")
    with context.lock:
        if data['currency'] == context.currency:
            params = ['symbol','qty','customer','side','settlement_ccy','avg_price','b2b_instrument','listed','b2b_bid']
            instrument,qty,booking_id,side,settlement_ccy,avg_price,b2b_instrument,listed,b2b_bid = [data[_] for _ in params]

            #! no b2b_instrument meaning no need to hedge
            if b2b_instrument:
                context.positions[booking_id] = {
                'symbol': instrument,
                'avg_price': avg_price,
                'qty': qty,
                'b2b_instrument': b2b_instrument,
                'listed': listed,
                'settlement_ccy': settlement_ccy,
                'side': side,
                'basket1': True
                }
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
                time_interval = (10 + dtm*365/180*(30-10))*60
                context.hedge_b2b_dict[booking_id] = {
                    'booking_id': booking_id,
                    'b2b_instrument' : b2b_instrument,
                    'target':target,
                    'b2b_bid':b2b_bid,
                    'listed':listed,
                    'time_interval' : time_interval,
                    'finished':False,
                    'status': 'slow'
                    #!status:1)slow 2)fast 3)move
                    #!move especially for when there is order still on the way but already need to move
                    #!thus in place order ready hedge_b2b_dict can finally be deleted
                }       
                best_bid, bid_amount = get_best_bid(b2b_instrument)
                if best_bid:
                    price = best_bid
                else:
                    best_ask = get_best_ask(b2b_instrument)[0]
                    if best_ask:
                        price = best_ask - context.min_tick
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
                context.logger.info(f"OrderQty: {OrderQty}, bid_amount:{amount}, price:{price},  time_interval:{time_interval}, dt: {dt/1000}")
                context.send_order(f'Deribit|{b2b_instrument}|option', TradeSide.Sell, price, OrderQty, OrderType.Fak, delay = dt, client_order_id = booking_id, note = note)
                
            context.logger.info(pd.DataFrame(context.positions).T)
            with open(f"data/{context.currency}_positions.pkl", "wb") as fw:
                pickle.dump(context.positions, fw)
                

def on_auto_hedge(context, data):
    pass


def on_target_update(context, data):
    pass


def on_signal_ready(context, data):
    context.logger.info(f'DemoStrategy::signal received')
    with context.lock:
        try:
            context.greek_updated = False
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
                    dtm = (exp-datetime.now()).total_seconds()/3600/24/365
                    vol = get_vol(data, dtm, float(strike))
                    fwdYield = interp(dtm, ttm, bitYield)
                    fwd_price = fwd*math.exp(fwdYield*dtm)

                    view = {}
                    eu_option = BS([fwd_price, float(strike), 0, dtm*365], volatility=vol*100)
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

            # for base in context.hedge_positions:
            #     #hedge_df = pd.DataFrame(context.hedge_positions[base]).T
            #     future_sym = base+"_perp"
            #     hedged_options = {key: value for key, value in context.hedge_positions[base].items() if key != future_sym}
            for key,values in context.hedge_positions[context.currency].items():
                currency, exp, strike, cp = key.split('_')[0].split('-')
                # currency, exp, strike, cp = key.split('-')
                if currency.find(data['currency']) != -1:
                    exp = datetime.strptime("{}".format(exp+" 16:00:00"), "%Y%m%d %H:%M:%S")
                    dtm = (exp-datetime.now()).total_seconds()/3600/24/365
                    vol = get_vol(data, dtm, float(strike))
                    fwdYield = interp(dtm, ttm, bitYield)
                    fwd_price = fwd*math.exp(fwdYield*dtm)

                    view = {}
                    eu_option = BS([fwd_price, float(strike), 0, dtm*365], volatility=vol*100)
                    view['iv'] = vol*100
                    price = eu_option.callPrice if cp == "C" else eu_option.putPrice
                    view['price'] = round(price/fwd, 4)
                    delta = eu_option.callDelta if cp == "C" else eu_option.putDelta
                    view['delta'] = round(delta, 4)
                    theta = eu_option.callTheta if cp == "C" else eu_option.putTheta
                    view['theta'] = round(theta, 4)
                    view['gamma'] = round(eu_option.gamma, 4)
                    view['vega'] = round(eu_option.vega, 4)
                    context.hedge_positions[context.currency][key].update(view)
            context.greek_updated = True
        except:
            context.logger.error(traceback.format_exc())
            #context.logger.info(f"Error - volData is:{data}")
    

# on_response_{action}_ready: 处理response的回调函数
def on_response_place_order_ready(context, data):
    context.logger.info(f'DemoStrategy::send order response received')
    context.logger.info(f'in place order {data}')
    metadata = data['metadata']
    error = "error_code" in metadata['metadata']
    if "inspect_resp"  in metadata['metadata']:
        time_out_error = "error_code"  in metadata['metadata']['inspect_resp']
    else:
        time_out_error = False
    note = metadata['request']['metadata']['note']
    try:
        if not (error or time_out_error):
            with context.lock:
                if not note['is_basket']:
                    booking_id = note['booking_id']
                    if booking_id in context.hedge_b2b_dict:
                        context.logger.info(f'Parent order:{context.hedge_b2b_dict[booking_id]}')
                    else:
                        return
                    b2b_instrument = context.hedge_b2b_dict[booking_id]['b2b_instrument']
                    listed = context.hedge_b2b_dict[booking_id]['listed']
                    quantity = metadata['request']['metadata']['quantity']
                    filled_quantity = metadata['request']['metadata']['filled_quantity']
                    avg_executed_price = metadata['request']['metadata']['avg_executed_price']
                    if 'action' in note:
                        #!fast order will only continue fast routine and only be listed
                        context.positions[booking_id]['b2b_qty'] += filled_quantity
                        context.positions[booking_id]['cum_qty'] -= (quantity - filled_quantity)
                        context.hedge_b2b_dict[booking_id]['target'] -= filled_quantity 

                        target_qty = context.hedge_b2b_dict[booking_id]['target'] 
                        if target_qty >= context.lot_size:
                            sum_bids = get_sum_bids(b2b_instrument)
                            if sum_bids:
                                amount = min(sum_bids,target_qty)
                                price = price_filter_by_volume(b2b_instrument, exclude_volume = amount, avg = False)
                            else:
                                amount = target_qty
                                best_ask = get_best_ask(b2b_instrument)[0]
                                if best_ask:
                                    price = best_ask - context.min_tick
                                else:
                                    price = get_mark_price(b2b_instrument)

                            context.send_order(f"Deribit|{b2b_instrument}|option", TradeSide.Sell, price, amount, OrderType.Fak, note = note)
                        else:
                            context.hedge_b2b_dict[booking_id]['finished'] = True
                            context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                            del context.hedge_b2b_dict[booking_id]
                    else:
                        #! if slow order just arrvied and status already changed
                        if context.hedge_b2b_dict[booking_id]['status'] != 'slow':
                            if quantity - filled_quantity > 0:
                                context.logger.error(f'cancel failed but not fully filled : {metadata}')
                                context.positions[booking_id]['b2b_qty'] -= (quantity - filled_quantity)
                                context.positions[booking_id]['cum_qty'] -= (quantity - filled_quantity)
                                #!in this case,b2b_qty already the same with cum_qty??
                            if context.hedge_b2b_dict[booking_id]['status'] == 'move':
                                context.hedge_b2b_dict[booking_id]['finished'] = True
                                context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                                context.positions[booking_id]['basket1'] = False
                                del context.hedge_b2b_dict[booking_id]
                                return
                            elif context.hedge_b2b_dict[booking_id]['status'] == 'fast':
                                return
                        else:
                            delay = metadata['request']['metadata']['delay']
                            context.positions[booking_id]['b2b_qty'] += filled_quantity
                            context.positions[booking_id]['cum_qty'] -= (quantity - filled_quantity)
                            context.hedge_b2b_dict[booking_id]['target'] -= filled_quantity if listed else filled_quantity*avg_executed_price
                            context.hedge_b2b_dict[booking_id]['time_interval'] -= filled_quantity/quantity*delay/1000

                            best_bid, bid_amount = get_best_bid(b2b_instrument)
                            
                            if best_bid:
                                price = best_bid
                                amount = bid_amount
                            else:
                                best_ask = get_best_ask(b2b_instrument)[0]
                                if best_ask:
                                    price = best_ask - context.min_tick
                                else:
                                    price = get_mark_price(b2b_instrument)
                                amount = context.hedge_b2b_dict[booking_id]['target'] if listed else context.hedge_b2b_dict[booking_id]['target']/price

                            target_qty = context.hedge_b2b_dict[booking_id]['target'] if listed else context.hedge_b2b_dict[booking_id]['target']/price
                            if target_qty < context.lot_size:
                                #!finished = true   solely for updating flow
                                context.hedge_b2b_dict[booking_id]['finished'] = True
                                #!this means fully hedged by b2b and will not be included in further baskets
                                context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                                if not listed:
                                    context.positions[booking_id]['basket1'] = False
                                del context.hedge_b2b_dict[booking_id]
                                return

                            sum_bids = get_sum_bids(b2b_instrument)
                            size_ratio = target_qty/sum_bids if sum_bids else 1
                            size_perc = 0.2 if size_ratio < 0.2 else max(1,size_ratio)
                            vol_perc = 1 if context.hedge_b2b_dict[booking_id]['b2b_bid']/price > 0.99 else size_perc
                            OrderQty = max(min(target_qty,amount*vol_perc),context.lot_size)

                            dt = max(int(np.ceil(OrderQty/target_qty*context.hedge_b2b_dict[booking_id]['time_interval'])),3)*1000 if vol_perc < 1 and OrderQty/target_qty <= 0.5 else 3000
                            context.send_order(f'Deribit|{b2b_instrument}|option', TradeSide.Sell, price, OrderQty, OrderType.Fak, delay = dt, client_order_id = booking_id, note = note)
                            context.logger.info(f"booking_id:{booking_id},target:{context.positions[booking_id]['qty']},remaining:{context.hedge_b2b_dict[booking_id]['target']},this time sent:{OrderQty},time_interval:{dt},b2b_bid:{context.hedge_b2b_dict[booking_id]['b2b_bid']},price:{price}")
                            context.intercom.emit(IntercomScope.OptionPosition, IntercomChannel.FlowUpdate, context.hedge_b2b_dict[booking_id])
                            context.positions[booking_id]['cum_qty'] += OrderQty
                else:
                    instrument = metadata['request']['metadata']['instrument_id']
                    if not instrument:
                        context.order_existed[note['basket']] = False
                    else:
                        del context.orders_existed[instrument]

                    context.update_order(metadata)
        else:
            context.logger.error(f"place order error:{metadata['metadata']['error_code_msg']}")
    except:
        context.logger.error(traceback.format_exc())  
        
def on_order_ready(context, data):
    pass


# on_position_ready: 处理position_info的回调函数
def on_position_ready(context, data):
    #context.logger.info(f'inside postion ready {data}')
    posInfoType = data['metadata']['posInfoType']
    if posInfoType == "future_position":
        # hedge_positions = data['metadata']['metadata']
        # for col in context.position_del_cols:
        #     del hedge_positions[col]

        # currency = data['metadata']['symbol']
        # future_sym = currency+"_perp"
        # if future_sym not in hedge_positions and future_sym in context.hedge_positions[currency]:
        #     del context.hedge_positions[currency][future_sym]

        # for key, value in hedge_positions.items():
        #     position = {}
        #     if hedge_positions[key]['buy_available'] > 0:
        #         position['side'] = 1
        #         position['qty'] = hedge_positions[key]['buy_available']
        #     else:
        #         position['side'] = -1
        #         position['qty'] = hedge_positions[key]['sell_available']

        #     if key in context.hedge_positions[currency]:
        #         context.hedge_positions[currency][key].update(position)
        #     else:
        #         context.hedge_positions[currency][key] = position

        if 'future_userinfo' in data['global_balances']:
            if context.currency == "BTCUSD":
                context.account_equity = data['global_balances']['future_userinfo']['BTC_rights']
                context.option_value = data['global_balances']['future_userinfo']['BTCUSD_option_value']
            else:
                context.account_equity = data['global_balances']['future_userinfo']['ETH_rights']
                context.option_value = data['global_balances']['future_userinfo']['ETHUSD_option_value']

    elif posInfoType == "option_position":
        hedge_positions = data['metadata']['metadata']
        for col in context.position_del_cols:
            del hedge_positions[col]

        currency = data['metadata']['symbol']
        cleared_positions = [key for key in context.hedge_positions[currency] if key+'_option' not in hedge_positions]

        for key in cleared_positions:
            # instrument = key.split('_')[0]
            context.hedge_positions[currency][key].update(context.empty_pos)

        for key, value in hedge_positions.items():
            option_position = {}
            option_position['average_price'] = value['average_price'] 
            option_position['floating_profit_loss'] = value['floating_profit_loss']
            instrument = key.split('_')[0]
            currency, exp, strike, cp = instrument.split('-')
            # if instrument in context.hedge_
            if value['buy_available'] > 0:
                option_position['side'] = 1
                option_position['qty'] = value['buy_available']
                option_position['group'] = exp
            else:
                option_position['side'] = -1
                option_position['qty'] = value['sell_available']
                option_position['group'] = exp

            if instrument in context.hedge_positions[currency]:
                context.hedge_positions[currency][instrument].update(option_position)
            else:
                context.hedge_positions[currency][instrument] = option_position


def update_order(context, data):
    context.logger.info(f'inside order upate:{data}')
    metadata = data['request']['metadata']
    if 'options' in metadata:
        del metadata['options']

    order_id = metadata['order_id']

    if metadata['contract_type'] == 'perp':
        note = metadata['note']
        basket = note['basket']
        
        # if order_id in context.orders:
        #     context.orders[order_id].update(metadata)
        #     side = 1 if context.orders[order_id]['direction'].lower() == 'buy' else -1
        #     quantity = context.orders[order_id]['quantity']
        #     symbol = context.orders[order_id]['symbol']
        # else:
        #     context.orders[order_id] = metadata
        #     side = 1 if metadata['direction'].lower() == 'buy' else -1
        #     quantity = metadata['quantity']
        #     symbol = metadata['symbol']

        status = metadata['status']
        filled_quantity = metadata['filled_quantity']
        side = 1 if metadata['direction'].lower() == 'buy' else -1

        if status == OrderStatus.Filled or status == OrderStatus.Cancelled:
            # key = symbol + "_perp"
            # target_qty = metadata['note']['original_qty'] * \
            #     metadata['note']['original_side'] + side*quantity
            # target_side = 1 if target_qty > 0 else -1
            # if key in context.hedge_positions[symbol]:
            #     context.hedge_positions[symbol][key]['qty'] = abs(target_qty)
            #     context.hedge_positions[symbol][key]['side'] = target_side
            # else:
            #     context.hedge_positions[symbol][key] = {'qty': abs(target_qty), 'side': target_side}

            context.future_delta[basket]['total'] += filled_quantity*side
            context.future_delta[basket]['filled'] = 0
            context.sent_orderId[basket] = None
        else:
            context.sent_orderId[basket] = order_id
            context.future_delta[basket]['filled'] = filled_quantity*side

        with open(f"data/future_delta.pkl", "wb" ) as fw:
            pickle.dump(context.future_delta, fw)

    else:
        if order_id in context.orders:
            context.orders[order_id].update(metadata)
            symbol = context.orders[order_id]['symbol']
            instrument = context.orders[order_id]['instrument_id']
        else:
            context.orders[order_id] = metadata
            symbol = metadata['symbol']
            instrument = metadata['instrument_id']

        exp_date = instrument.split('-')[1]
        status = metadata['status']
        if status == OrderStatus.Filled or status == OrderStatus.Cancelled:
            #! using deribit positon is enough i guess??
            target_qty = metadata['note']['target_qty']
            target_side = metadata['note']['target_side']
            if instrument in context.hedge_positions[symbol]:
                # context.hedge_positions[symbol][instrument]['qty'] = target_qty
                # context.hedge_positions[symbol][instrument]['side'] = target_side
                pass
            else:
                context.hedge_positions[symbol][instrument] = {'qty': target_qty, 'side': target_side}

            if instrument in context.sent_orders_dict[exp_date]:
                del context.sent_orders_dict[exp_date][instrument]
        else:
            context.sent_orders_dict[exp_date][instrument] = order_id


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
            dtm = (exp - datetime.now()).total_seconds()/3600/24
            if dtm <= 0:
                del context.positions[key]

    instrument = get_last_instrument(context.currency)   
    context.query_orders(f'Deribit|{context.currency}|perp')
    context.query_orders(f'Deribit|{instrument}|option')


def basket2_vega_hedge(context):
    warm_up_time = (datetime.now() - context.strgy_start_time).total_seconds()/60
    if not(context.group_updated and warm_up_time > 0.5):
        return
    currency = context.currency
    positions = pd.DataFrame(context.positions).T
    hedge_positions = pd.DataFrame(context.hedge_positions[currency]).T
    positions = positions[~positions['group'].isin(context.near_expiry_dates)]
    hedge_option_df = hedge_positions[~hedge_positions['group'].isin(context.near_expiry_dates)]
    option_df = positions[positions['basket1'] == False]
    basket1 = positions[positions['basket1'] == True]
    if not basket1.empty:
        basket1_gp = basket1.groupby(['b2b_instrument'])
        basket1_b2b_qty = basket1_gp['qty'].sum().to_dict()
        for key,value in basket1_b2b_qty:
            if key in hedge_option_df.index.values:
                hedge_option_df.loc[hedge_option_df.index == key,'qty'] -= value['qty']
        null_qty_index = hedge_option_df[hedge_option_df['qty'] == 0].index
        
    listed_option_df = option_df[option_df['listed'] == True]
    if not listed_option_df.empty:
        listed_option_gp = listed_option_df.groupby(['b2b_instrument'])
        listed_option_b2b_qty = listed_option_gp['qty'].sum().to_dict()
        for key,value in listed_option_b2b_qty:
            if key in hedge_option_df.index.values:
                hedge_option_df.loc[hedge_option_df.index == key,'qty'] -= value['qty']
        option_df.loc[option_df['listed'] == True,'qty'] = option_df['qty'] - option_df['cum_qty']

    null_qty_index = hedge_option_df[hedge_option_df['qty'] == 0].index
    hedge_option_df.drop(null_qty_index, inplace=True)
    #!to avoid unnecessary group generated 


    account_gamma = 0
    account_vega = 0
    exposure_gamma = 0
    deribit_gamma = 0
    exposure_vega = 0
    deribit_vega = 0

    option_group = option_df['group'].unqiue().tolist()
    hedge_option_group = hedge_option_df['group'].unqiue().tolist()
    groups = set(option_group + hedge_option_group)
    
    for group in groups:
        hedge_group_opiton_df = hedge_option_df[hedge_option_df['group'] == group]
        basket_option_df = option_df[option_df['group'] == group]

        # 因为我们是客户的对手方所以需要乘以-1
        if not basket_option_df.empty:
            basket_bs_gamma = -1 * (basket_option_df['qty'] * \
                    basket_option_df['side'] * basket_option_df['gamma']).sum()
            basket_bs_vega = -1 * (basket_option_df['qty'] * \
                   basket_option_df['side'] * basket_option_df['vega']).sum()
        else:
            basket_bs_gamma = 0
            basket_bs_vega = 0

        exposure_gamma += basket_bs_gamma
        exposure_vega += basket_bs_vega

        if not hedge_group_opiton_df.empty:
            current_hedge_gamma = (hedge_group_opiton_df['qty'] * \
                    hedge_group_opiton_df['side']*hedge_group_opiton_df['gamma']).sum()
            current_hedge_vega = (hedge_group_opiton_df['qty'] * \
                    hedge_group_opiton_df['side']*hedge_group_opiton_df['vega']).sum()
        else:
            current_hedge_gamma = 0
            current_hedge_vega = 0
            

        deribit_gamma += current_hedge_gamma
        group_gamma = basket_bs_gamma + current_hedge_gamma
        account_gamma += group_gamma   
        deribit_vega += current_hedge_vega
        group_vega = basket_bs_vega + current_hedge_vega
        account_vega += group_vega
        context.logger.info(f"[THIS IS GROUP]:{group}, group_gamma:{group_gamma}, group_vega:{group_vega}")

        group_qty = option_df[option_df['group'] == group]['qty'].sum() 
        data_dict = context.generate_basket(context.currency, group, group_qty, group_gamma, group_vega, eq = 'vega')


        context.logger.info(f"[basket2] group_gamma is {group_gamma},group_vega is {group_vega},data_dict:{data_dict}")
        if context.group_updated and warm_up_time > 0.5:
            if group_vega < 0 and abs(group_vega) > context.vega_maker_limit:
                context.logger.info("[basket2] buying in maker limit beach")
                context.hedge_execution(group, group_gamma, group_vega, data_dict)
            if group_vega > 0 and abs(group_vega) > context.vega_taker_limit:
                context.logger.info("[basket2] selling in taker limit beach")
                context.hedge_execution(group, group_gamma, group_vega, data_dict)
            

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


def basket2_delta_hedge(context):
    basket = 'basket2'
    with context.lock:
        context.logger.info(f'{basket}_delta_hedge_time:{datetime.now()}')
        minutes_diff = (datetime.now() - context.delta_hedge_time[basket]).total_seconds()/60
        warm_up_time = (datetime.now() - context.strgy_start_time).total_seconds()/60
        try:
            positions = pd.DataFrame(context.positions).T
            positions = positions[~positions['group'].isin(context.near_expiry_dates)]
            position_df = positions[positions['basket1'] == False]
            basket1 = positions[positions['basket1'] == True]

            if context.account_equity != 0 and ("delta" in position_df or len(position_df) == 0):
                future_data = get_instrument_summaryinfo(context.currency)
                
                future_delta = sum(context.future_delta[basket].values())
                future_qty = float(np.abs(future_delta))
                future_side = np.sign(future_delta)
                
                hedged_options = pd.DataFrame(context.hedge_positions[context.currency]).T
                hedged_options_df = hedged_options[~hedged_options['group'].isin(context.near_expiry_dates)]

                if not basket1.empty:
                    basket1_gp = basket1.groupby(['b2b_instrument'])
                    basket1_b2b_qty = basket1_gp['qty'].sum()
                    for key,value in basket1_b2b_qty:
                        if key in hedged_options_df.index.values:
                            hedged_options_df.loc[hedged_options_df.index == key,'qty'] -= value['qty']

                listed_option_df = position_df[position_df['listed'] == True]
                if not listed_option_df.empty:
                    listed_option_gp = listed_option_df.groupby(['b2b_instrument'])
                    listed_option_b2b_qty = listed_option_gp['qty'].sum().to_dict()
                    for key,value in listed_option_b2b_qty:
                        if key in hedged_options_df.index.values:
                            hedged_options_df.loc[hedged_options_df.index == key,'qty'] -= value['qty']
                    position_df.loc[position_df['listed'] == True,'qty'] = position_df['qty'] - position_df['cum_qty']


                if len(hedged_options_df) > 0 and "delta" in hedged_options_df:
                    hedged_options_delta = (hedged_options_df['delta'] * hedged_options_df['qty'] * hedged_options_df['side']).sum()
                else:
                    hedged_options_delta = 0
                

                if len(position_df) == 0:
                    usd_option_delta = 0
                    cryto_option_delta = 0
                else:
                    cryto_settle_options = position_df[position_df['settlement_ccy'] != 'USD']
                    usd_settle_options = position_df[position_df['settlement_ccy'] == 'USD']
                    # 因为我们是客户的对手方所以需要乘以-1
                    usd_option_delta = -1 * (usd_settle_options['side'] * \
                           usd_settle_options['qty'] * usd_settle_options['delta']).sum()
                    cryto_option_bs_delta = -1 * (cryto_settle_options['side'] * \
                           cryto_settle_options['qty'] * cryto_settle_options['delta']).sum()
                    cryto_option_premium = -1 * (cryto_settle_options['side'] * \
                           cryto_settle_options['qty'] * cryto_settle_options['avg_price']).sum()
                    cryto_option_delta = cryto_option_bs_delta - cryto_option_premium

                hedged_options_bs_delta = hedged_options_delta - context.option_value
                customer_options_delta = usd_option_delta + cryto_option_delta
                total_option_delta = hedged_options_bs_delta + customer_options_delta
                # current_account_delta = context.account_equity + total_option_delta + future_delta
                # delta_diff = context.account_target - current_account_delta
                delta_diff = - total_option_delta - future_delta
                # context.logger.info(f"basket2 time diff is {minutes_diff},current_account_delta is {current_account_delta}, delta total is {round(delta_diff,4)}")
                context.logger.info(f"{basket} time diff is {minutes_diff}, delta total is {round(delta_diff,4)}")

                if context.group_updated and type(delta_diff) != complex and warm_up_time > 0.5:
                    note = {'aggresive': "maker", "original_qty": future_qty, "original_side": future_side, 'basket':basket}
                    if minutes_diff > context.delta_time_limit_basket2:
                        context.logger.info(f"{basket} in minutes_diff")
                        context.order_execution(future_data, delta_diff, note)
                        context.delta_hedge_time[basket] = datetime.now()
                        delta_data = {'hedge_time': context.delta_hedge_time[basket],'account_target': context.account_target}
                        with open(f'data/{basket}_{context.currency[:3]}_delta_data.pkl', 'wb') as fw:
                            pickle.dump(delta_data, fw)
                    elif abs(delta_diff) > context.delta_limit_maker_basket2 and abs(delta_diff) < context.delta_limit_taker_basket2:
                        context.logger.info(f"{basket} in limit_maker")
                        context.order_execution(future_data, delta_diff, note)
                    elif abs(delta_diff) > context.delta_limit_taker_basket2:
                        context.logger.info(f"{basket} in limit_taker")
                        note['aggresive'] = 'taker'
                        context.order_execution(future_data, delta_diff, note)
                    elif not context.sent_orderId[basket] is None:
                        context.logger.info(f"{basket} in exsiting order")
                        context.order_execution(future_data, delta_diff, note)

                account_data = {
                    'type': 'delta',
                    'basket':2,
                    f'{context.currency[:3]}':{
                            'target': '%.4f' % context.account_target,
                            'equity': '%.4f' % context.account_equity,
                            'hedged_options_delta': '%.4f' % hedged_options_bs_delta,
                            'customer_options_delta': '%.4f' % customer_options_delta,
                            'account_option_delta': '%.4f' % total_option_delta,
                            'future_delta': '%.4f' % future_delta,
                            'account_delta': '%.4f' % (-delta_diff)
                            }
                }
                context.send_data_to_user(account_data)


        except:
            context.logger.error(traceback.format_exc())


def order_execution(context, market_data, delta_diff, note):
    basket = note['basket']
    if delta_diff == 0 or context.order_existed[basket]:
        return
    # If there is an existing order:
    # If it's a taker order, cancel it directly
    # If it's a maker order, cancel it if the price does not match the best price or the side is changed
    context.logger.info(f'{basket} inside order_excution, sent order id is {context.sent_orderId[basket]}')
    if not context.sent_orderId[basket] is None:
        open_order = context.orders[context.sent_orderId[basket]]
        context.logger.info(f'open_order is {open_order}')
        if open_order['note']['aggresive'] == 'maker':
            order_side = open_order['direction']
            if delta_diff > 0:
                side = Direction.Buy
                best_price = market_data['best_bid']
            else:
                side = Direction.Sell
                best_price = market_data['best_ask']

            context.logger.info(f'there is an exsting order, side:{side}, order_side:{order_side}, best_price:{best_price},open_price:{open_order["price"]}')
            if side.lower() != order_side.lower() or best_price != open_order['price']:
                context.cancel_order(f'Deribit|{context.currency}|perp', context.sent_orderId[basket], delta_diff=delta_diff, note=note)
        else:
            context.cancel_order(f'Deribit|{context.currency}|perp', context.sent_orderId[basket], delta_diff=delta_diff, note=note)
    else:
        # If there has no existing order, send order to the market
        context.send_limit_order(market_data = market_data, delta_diff = delta_diff, note = note)

def update_group(context):
    context.logger.info(f'Group date updated at {datetime.now()}')
    enough_time_diff = (datetime.now() - context.last_group_update_time).total_seconds() >= 60
    if not enough_time_diff:
        return
    try:
        with context.lock:
            context.group_updated = False
            context.near_expiry_dates = get_near_expiry_dates(context.currency)
            for key,value in context.positions.items():
                exp = value['symbol'].split('-')[1]
                new_group = get_group_date(exp,context.currency)
                if context.positions[key]['group'] != new_group:
                    context.logger.info(f'{context.positions[key]} Group date updated to {new_group} at {datetime.now()}')
                    context.positions[key]['group'] = new_group
                    # if new_group in context.near_expiry_dates:
                    #     context.near_expiry_positions.update(context.positions[key])
                        # del context.basket2_positions[key]

            # for key,value in context.hedge_positions[context.currency].items():
            #     exp = value['symbol'].split('-')[1]
            #     new_group = get_group_date(exp,context.currency)
            #     if context.hedge_positions[context.currency][key]['group'] != new_group:
            #         context.logger.info(f'{context.hedge_positions[context.currency][key]} Group date updated to {new_group} at {datetime.now()}')
            #         context.hedge_positions[context.currency][key]['group'] = new_group
            #         if new_group in near_expiry_dates:
            #             context.near_expiry_hedge_positions.update(context.hedge_positions[context.currency][key])
            #             del context.basket2_hedge_positions[context.currency][key]
            # context.logger.info(f'near expiry hedged positions: {context.near_expiry_hedge_positions}')
            # context.logger.info(f'near expiry customer positions: {context.near_expiry_positions}')
            context.group_updated = True
            context.last_group_update_time = datetime.now()
    except:
        context.group_updated = False
        context.logger.info(f'Near expiry dates:{context.near_expiry_dates}')
        context.logger.error(traceback.format_exc())
        


def market_monitor(context):
    with context.lock:
        for key,value in context.hedge_b2b_dict.items():

            b2b_bid_now = price_filter_by_volume(value['b2b_instrument'], avg = True)
            if not b2b_bid_now:
                b2b_ask_now = get_best_ask(value['b2b_instrument'])[0]
                if not b2b_ask_now:
                    b2b_price_now = get_mark_price(value['b2b_instrument'])
                else:
                    b2b_price_now = b2b_ask_now - context.min_tick
            else:
                b2b_price_now = b2b_bid_now

            note = {'is_basket': False, 'booking_id': key}
            if value['listed']:
                if 0.85 < value['b2b_bid']/b2b_price_now <= 1:
                    note['action'] = 'monitor_order'
                    if context.hedge_b2b_dict[key]['status'] == 'slow':
                        context.hedge_b2b_dict[key]['status'] = 'fast'
                        if key in context.open_b2b_orders:
                            filled_quantity = context.open_b2b_orders[key]['filled']
                            #TODO this quantity might not be fianlly filled
                            #?target should  not be  updated here maybe??
                            value['target'] -= filled_quantity if value['listed'] else filled_quantity*value['avg_executed_price']
                            context.cancel_order(f"Deribit|{value['b2b_instrument']}|option",context.open_b2b_orders[key]['order_id'], note = note)
                        
                        else:
                            cum_qty = context.positions[key]['cum_qty']
                            target_qty = value['target'] - cum_qty if value['listed'] else value['target']/b2b_price_now - cum_qty
                            
                            #!already filled but not open,target-cum_qty(all filled qty plus this order quantity)
                            if target_qty >= context.lot_size:
                                context.send_order(f"Deribit|{value['b2b_instrument']}|option", TradeSide.Sell, b2b_price_now, target_qty, OrderType.Fak, note = note)
                                #?if this price suitable for fak?
                    else:
                        context.logger.info(f'{context.hedge_b2b_dict[key]} is already in fast process')

                elif 1 < value['b2b_bid']/b2b_price_now:
                    note['action'] = 'move_to_basket'
                    if context.hedge_b2b_dict[key]['status'] == 'slow':
                        context.hedge_b2b_dict[key]['status'] = 'move'
                        if key in context.open_b2b_orders:
                            filled_quantity = context.open_b2b_orders[key]['filled']
                            value['target'] -= filled_quantity if value['listed'] else filled_quantity*value['avg_executed_price']
                            context.cancel_order(f"Deribit|{value['b2b_instrument']}|option",context.open_b2b_orders[key]['order_id'], note = note)
                        else:
                            cum_qty = context.positions[key]['cum_qty']
                            target_qty = value['target'] - cum_qty if value['listed'] else value['target']/b2b_price_now - cum_qty
                            #!already filled but not open,target-cum_qty(all filled qty plus this order quantity)
                            #?context.positions using cum_qty already,is this step needed?
                    elif context.hedge_b2b_dict[key]['status'] == 'fast':
                        context.hedge_b2b_dict[key]['status'] = 'move'
                        cum_qty = context.positions[key]['cum_qty']
                        target_qty = value['target'] - cum_qty if value['listed'] else value['target']/b2b_price_now - cum_qty

            else:
                if 0.9 < value['b2b_bid']/b2b_price_now:
                    context.hedge_b2b_dict[key]['status'] = 'move'
                    #! if unlisted, no need to cancel,movev to basket2 directly
                    context.positions[key]['basket1'] = False
                    

def on_response_query_orders_ready(context, data):
    context.logger.info(data)
    with context.lock:
        metadata = data['metadata']['metadata']
        orders = metadata['orders']
        context.open_b2b_orders = {v['client_order_id']:v for v in orders if v['client_order_id'] != ''}
        with open(f"data/query_orders.pkl", "wb") as fw:
            pickle.dump(context.open_b2b_orders, fw)


    # for value in orders:
    #     if value['client_order_id']:
    #         context.open_b2b_orders[value['client_order_id']] = value
        # else:
        #     context.open_hedge_orders.update({value['instrument_id']: value})


def basket3_gamma_hedge(context):

    warm_up_time = (datetime.now() - context.strgy_start_time).total_seconds()/60
    if not(context.group_updated and warm_up_time > 0.5):
        return
    currency = context.currency
    positions = pd.DataFrame(context.positions).T
    hedge_positions = pd.DataFrame(context.hedge_positions[currency]).T
    positions = positions[positions['group'].isin(context.near_expiry_dates)]
    hedge_option_df = hedge_positions[hedge_positions['group'].isin(context.near_expiry_dates)]

    option_df = positions[positions['basket1'] == False]
    basket1 = positions[positions['basket1'] == True]
    if not basket1.empty:
        basket1_gp = basket1.groupby(['b2b_instrument'])
        basket1_b2b_qty = basket1_gp['qty'].sum().to_dict()
        for key,value in basket1_b2b_qty:
            if key in hedge_option_df.index.values:
                hedge_option_df.loc[hedge_option_df.index == key,'qty'] -= value['qty']
        
    listed_option_df = option_df[option_df['listed'] == True]
    if not listed_option_df.empty:
        listed_option_gp = listed_option_df.groupby(['b2b_instrument'])
        listed_option_b2b_qty = listed_option_gp['qty'].sum().to_dict()
        for key,value in listed_option_b2b_qty:
            if key in hedge_option_df.index.values:
                hedge_option_df.loc[hedge_option_df.index == key,'qty'] -= value['qty']
        option_df.loc[option_df['listed'] == True,'qty'] = option_df['qty'] - option_df['cum_qty']

    null_qty_index = hedge_option_df[hedge_option_df['qty'] == 0].index
    hedge_option_df.drop(null_qty_index, inplace=True)
    #! to avoid unnecessary group generated

    account_gamma = 0
    account_vega = 0
    exposure_gamma = 0
    deribit_gamma = 0
    exposure_vega = 0
    deribit_vega = 0
    option_group = option_df['group'].unqiue().tolist()
    hedge_option_group = hedge_option_df['group'].unqiue().tolist()
    groups = set(option_group + hedge_option_group)

    today = datetime.now().date().strftime("%Y%m%d")
    if today in groups:
        groups.remove(today)

    for group in groups:
        hedge_group_opiton_df = hedge_option_df[hedge_option_df['group'] == group]
        basket_option_df = option_df[option_df['group'] == group]

        # 因为我们是客户的对手方所以需要乘以-1
        if not basket_option_df.empty:
            basket_bs_gamma = -1 * (basket_option_df['qty'] * \
                    basket_option_df['side'] * basket_option_df['gamma']).sum()
            basket_bs_vega = -1 * (basket_option_df['qty'] * \
                   basket_option_df['side'] * basket_option_df['vega']).sum()
        else:
            basket_bs_gamma = 0
            basket_bs_vega = 0

        exposure_gamma += basket_bs_gamma
        exposure_vega += basket_bs_vega

        if not hedge_group_opiton_df.empty:
            current_hedge_gamma = (hedge_group_opiton_df['qty'] * \
                hedge_group_opiton_df['side']*hedge_group_opiton_df['gamma']).sum()
            current_hedge_vega = (hedge_group_opiton_df['qty'] * \
                hedge_group_opiton_df['side']*hedge_group_opiton_df['vega']).sum()
        else:
            current_hedge_gamma = 0
            current_hedge_vega = 0
            

        deribit_gamma += current_hedge_gamma
        group_gamma = basket_bs_gamma + current_hedge_gamma
        account_gamma += group_gamma   
        deribit_vega += current_hedge_vega
        group_vega = basket_bs_vega + current_hedge_vega
        account_vega += group_vega
        context.logger.info(f"[THIS IS GROUP]:{group}, group_gamma:{group_gamma}, group_vega:{group_vega}")
        group_qty = option_df[option_df['group'] == group]['qty'].sum() 
        #? group_qty postitive??
        data_dict = context.generate_basket(context.currency, group, group_qty, group_gamma, group_vega, eq = 'gamma')


        context.logger.info(f"[basket3] group_gamma is {group_gamma},group_vega is {group_vega},data_dict:{data_dict}")
        
        if context.group_updated and warm_up_time > 0.5:
            
            exp = datetime.strptime("{}".format(group+" 16:00:00"), "%Y%m%d %H:%M:%S")
            dtm = ((exp-datetime.now()).total_seconds()/3600/24)

            if dtm > 1:
                if group_gamma < 0 and abs(group_gamma) > context.gamma_maker_limit:
                    context.logger.info("[basket3][dtm>1] buying in maker limit beach")
                    context.hedge_execution(group, group_gamma, group_vega, data_dict)
                if group_gamma > 0 and abs(group_gamma) > context.gamma_taker_limit:
                    context.logger.info("[basket3][dtm>1] selling in taker limit beach")
                    context.hedge_execution(group, group_gamma, group_vega, data_dict)
            else:
                if group_gamma < 0 and abs(group_gamma) > 3*context.gamma_maker_limit:
                    context.logger.info("[basket3][dtm<1] buying in maker limit beach")
                    context.hedge_execution(group, group_gamma, group_vega, data_dict)
                if group_gamma > 0 and abs(group_gamma) > 2*context.gamma_taker_limit:
                    context.logger.info("[basket3][dtm<1] selling in taker limit beach")
                    context.hedge_execution(group, group_gamma, group_vega, data_dict)


def basket3_delta_hedge(context):
    basket = 'basket3'
    with context.lock:
        context.logger.info(f'basket3_delta_hedge:{datetime.now()}')
        minutes_diff = (datetime.now() - context.delta_hedge_time[basket]).total_seconds()/60
        warm_up_time = (datetime.now() - context.strgy_start_time).total_seconds()/60
        try:
            positions = pd.DataFrame(context.positions).T
            position_df = positions[positions['group'].isin(context.near_expiry_dates)]
            position_df = positions[positions['basket1'] == False]
            basket1 = positions[positions['basket1'] == True]


            if context.account_equity != 0 and ("delta" in position_df or len(position_df) == 0):
                # future_data = context.symbol_dict[context.currency]
                future_data = get_instrument_summaryinfo(context.currency)
                #context.logger.info(f'inside hedge delta:{context.hedge_positions["BTCUSD"]}')
                future_delta = sum(context.future_delta[basket].values())
                future_qty = float(abs(future_delta))
                future_side = np.sign(future_delta)
                
                hedged_options = pd.DataFrame(context.hedge_positions[context.currency]).T
                hedged_options_df = hedged_options[hedged_options['group'].isin(context.near_expiry_dates)]

                if not basket1.empty:
                    basket1_gp = basket1.groupby(['b2b_instrument'])
                    basket1_b2b_qty = basket1_gp['qty'].sum()
                    for key,value in basket1_b2b_qty:
                        if key in hedged_options_df.index.values:
                            hedged_options_df.loc[hedged_options_df.index == key,'qty'] -= value['qty']

                listed_option_df = position_df[position_df['listed'] == True]
                if not listed_option_df.empty:
                    listed_option_gp = listed_option_df.groupby(['b2b_instrument'])
                    listed_option_b2b_qty = listed_option_gp['qty'].sum().to_dict()
                    for key,value in listed_option_b2b_qty:
                        if key in hedged_options_df.index.values:
                            hedged_options_df.loc[hedged_options_df.index == key,'qty'] -= value['qty']
                    position_df.loc[position_df['listed'] == True,'qty'] = position_df['qty'] - position_df['cum_qty']


                if len(hedged_options_df) > 0 and "delta" in hedged_options_df:
                    hedged_options_delta = (hedged_options_df['delta'] * hedged_options_df['qty'] * hedged_options_df['side']).sum()
                else:
                    hedged_options_delta = 0

                if len(position_df) == 0:
                    usd_option_delta = 0
                    cryto_option_delta = 0
                else:
                    cryto_settle_options = position_df[position_df['settlement_ccy'] != 'USD']
                    usd_settle_options = position_df[position_df['settlement_ccy'] == 'USD']
                    # 因为我们是客户的对手方所以需要乘以-1
                    usd_option_delta = -1 * (usd_settle_options['side'] * \
                           usd_settle_options['qty'] * usd_settle_options['delta']).sum()
                    cryto_option_bs_delta = -1 * (cryto_settle_options['side'] * \
                           cryto_settle_options['qty'] * cryto_settle_options['delta']).sum()
                    cryto_option_premium = -1 * (cryto_settle_options['side'] * \
                           cryto_settle_options['qty'] * cryto_settle_options['avg_price']).sum()
                    cryto_option_delta = cryto_option_bs_delta - cryto_option_premium

                hedged_options_bs_delta = hedged_options_delta - context.option_value
                customer_options_delta = usd_option_delta + cryto_option_delta
                total_option_delta = hedged_options_bs_delta + customer_options_delta

                delta_diff = - total_option_delta - future_delta

                context.logger.info(f"{basket} time diff is {minutes_diff} ,delta total is {round(delta_diff,4)}")

                if context.group_updated and type(delta_diff) != complex and warm_up_time > 0.5:
                    note = {'aggresive': "maker", "original_qty": future_qty, "original_side": future_side,'basket':basket}
                    if minutes_diff > context.delta_time_limit_basket3:
                        context.logger.info(f"{basket} in minutes_diff")
                        context.order_execution(future_data, delta_diff, note)
                        context.delta_hedge_time[basket] = datetime.now()
                        delta_data = {'hedge_time': context.delta_hedge_time[basket],'account_target': context.account_target}
                        with open(f'data/{basket}_{context.currency[:3]}_delta_data.pkl', 'wb') as fw:
                            pickle.dump(delta_data, fw)
                    elif abs(delta_diff) > context.delta_limit_maker_basket3 and abs(delta_diff) < context.delta_limit_taker_basket3:
                        context.logger.info(f"{basket} in limit_maker")
                        context.order_execution(future_data, delta_diff, note)
                    elif abs(delta_diff) > context.delta_limit_taker_basket3:
                        context.logger.info(f"{basket} in limit_taker")
                        note['aggresive'] = 'taker'
                        context.order_execution(future_data, delta_diff, note)
                    elif not context.sent_orderId[basket] is None:
                        context.logger.info(f"{basket} in exsiting order")
                        context.order_execution(future_data, delta_diff, note)

                account_data = {
                    'type': 'delta',
                    'basket':2,
                    f'{context.currency[:3]}':{
                            'target': '%.4f' % context.account_target,
                            'equity': '%.4f' % context.account_equity,
                            'hedged_options_delta': '%.4f' % hedged_options_bs_delta,
                            'customer_options_delta': '%.4f' % customer_options_delta,
                            'account_option_delta': '%.4f' % total_option_delta,
                            'future_delta': '%.4f' % future_delta,
                            'account_delta': '%.4f' % (-delta_diff)
                            }
                }
                context.send_data_to_user(account_data)

        except:
            context.logger.error(traceback.format_exc())


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
        
        #Handle send orders in add_syms, if no out_syms orders needed to be canceled, send order.
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
                    context.send_limit_order(symbol = sym, market_data = data_dict[sym], note = note)
        
        #Handle cancel orders in out_syms
        context.logger.info(f'Canceling orders,out_syms:{out_syms},context.sent_orders_dict:{context.sent_orders_dict}')
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
                else:
                    side = Direction.Sell
                    best_price = data_dict[sym]['best_ask']

                context.logger.info(f'inside order_excution, there is an exsting order, side:{side},\
                    order_side:{order_side}, best_price:{best_price},open_price:{open_order["price"]}')

                if side.lower() != order_side.lower():
                    context.cancel_order(f'Deribit|{sym}|option', sent_order_id, note=note)
                elif best_price != open_order['price']:
                    note['aggresive'] = 'maker'
                    context.modify_order(f'Deribit|{sym}|option', sent_order_id, best_price, abs(diff), note=note)
            else:
                context.cancel_order(f'Deribit|{sym}|option', sent_order_id, note=note)
