import sys
import os

parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)
    
import json
import pickle
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from library import get_strategy
from utils.util_func import *
from optparse import OptionParser

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = OptionParser()
    parser.add_option('-f', '--file_name', action='store', type='string', default=None)
 
    (opts, args) = parser.parse_args()
    file_path = f'library/strategy/{opts.file_name}.json'
    strategy_data_file = opts.file_name.split('_')[0]+"_data"
    with open(file_path, 'r') as f:
        options = json.load(f)
    '''
    from datetime import datetime
    import pandas as pd
    import pickle
    positions = pd.read_csv("data/positions_s.csv")
    positions['group'] = positions['group'].astype(str)
    #hedge_positions = pd.read_csv("data/hedge_positions.csv",index_col=0)
    #hedge_positions['group'] = hedge_positions['group'].astype(str)
    strategy_data = {'hedge_time':datetime.now()}
    with open(f'data/delta_data.pkl','wb') as fw:
        pickle.dump(strategy_data, fw)
    
    with open(f'data/customer_position.pkl','wb') as fw:
        pickle.dump(positions, fw)
    
    today = datetime.now()
    cols = ['EXP_DATE','ask_price', 'bid_price', 'creation_timestamp','instrument_name', 'K','S','cp',
                            'interest_rate','open_interest','underlying_index', 'volume','TTM']
    option_df = pd.read_csv("data/option_df.csv",index_col=0)
    option_df = option_df[cols]
    #option_df['TTM'] = [days_diff(exp_date,today) for exp_date in option_df['EXP_DATE']] 
    option_df = option_df[option_df['TTM']>0.1]
    portfolio = sim_positions(option_df,6)

    subscription_list = [symbol2subs(symbol,"%d%b%y") for symbol in portfolio['instrument_name']]
    '''
    with open(f'data/{strategy_data_file}.pkl','rb') as fw:
        strategy_data = pickle.load(fw)  
    
    with open(f'data/customer_position.pkl','rb') as fw:
        positions = pickle.load(fw) 

    positions,is_removed = remove_expired_positions(positions) 
    if is_removed:
        with open(f'data/customer_position.pkl','wb') as fw:
            pickle.dump(positions, fw)

    hedge_time = strategy_data['hedge_time']
    #hedge_positions = strategy_data['hedge_positions']
    #positions = {key:{k:0 for k,v in values.items()} for key,values in positions.items()}
    #subscription_list = [symbol2subs(symbol,"%Y%m%d") for symbol in positions.keys() if symbol!='BTCUSD']
    subscription_list = []
    subscription_list.append('Deribit|BTCUSD|perp|ticker')
    subscription_list.append('Deribit|BTCUSD|option|summaryinfo')
    
    options['subscription_list'] = list(set(subscription_list))
    options['hedge_time'] = hedge_time
    options['positions'] = positions
    if strategy_data_file == "delta_data":
        options['account_target'] = float(strategy_data['account_target'])

    stratgy = options['file_name']
    context = get_strategy(stratgy)
    context.logger.info('Start trading..')
    context.config_update(**options)
    
    context.pre_start(**options)
    context.start()

    #instrument = 'Deribit|BTCUSD-20200925-7000-P|option'
    #instrument = 'Deribit|BTCUSD|option|summaryinfo'
    #instrument = 'Deribit|BTCUSD|perp'
    #context.send_order(instrument, 'sell', 0.1200, 0.1, 'Limit')
    #context.send_order(instrument, 'sell', 0.1, 0.1, 'Fak', delay=3000)
    #context.send_order(instrument, 'sell', 9500.5, 1, 'Limit',note='maker')
    #context.send_order(instrument, 'buy', 8100.5, 1, 'Market',note='taker')
    #context.inspect_order(instrument,'3887280714') 
    #context.send_order(instrument,'buy',7084,0.0706,'Limit')

    
    
