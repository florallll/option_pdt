from datetime import datetime

import sys
import os
parent_path = os.path.dirname(sys.path[0])
if parent_path not in sys.path:
    sys.path.append(parent_path)

from configs import SYMBOL_MAP, FEE_RATE
import pandas as pd
import numpy as np
import json
from qpython import qconnection,MetaData
from qpython.qtype import QKEYED_TABLE
import QuantLib as ql

def time_tango(date):
        return datetime.strptime("{}".format(date), "%d%b%y")

def py2ql_date(pydate):
    
    return ql.Date(pydate.day,pydate.month,pydate.year)

def getYearFrac(date0, date1):

    day_count=ql.Actual365Fixed()
    yrs = day_count.yearFraction(py2ql_date(date0), py2ql_date(date1))
    
    if yrs == 0:
        yrs += 0.001
        
    return yrs

def split_symbol(symbol):
    return SYMBOL_MAP[symbol]


def get_human_readable_timestamp():
    return datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]


def get_timestamp():
    return datetime.now().timestamp()


def get_fee_rates(exchange, contract_type):
    fee_rates = FEE_RATE[exchange.lower()]
    if contract_type in fee_rates:
        return fee_rates[contract_type]
    else:
        return fee_rates['future']


def coin2instrument(coin):
    return '|'.join([coin.split("_")[1], coin.split("_")[0], "_".join(coin.split("_")[2:])])


def instrument2coin(instrument):
    return '_'.join([instrument.split("|")[1], instrument.split("|")[0], instrument.split("|")[2]])


def getATMstk(F0,Klist):
    
    K0=Klist[0]
    Kdis=abs(K0-F0)
    K0pos=0
    
    for i in range(0,len(Klist)):
        if abs(Klist[i]-F0)<Kdis:
            K0pos=i
            K0=Klist[i]
            Kdis=abs(Klist[i]-F0)
            
    return K0

def sim_positions(optData,holding_number):

    optData=optData.sort_values('TTM')
    optExp = pd.Series(optData['EXP_DATE'].values.ravel()).unique()
    opt_df = pd.DataFrame()
    for i in range(0,len(optExp)):
        optDataSub=optData.loc[optData['EXP_DATE']==optExp[i]]
        optDataSub = optDataSub.sort_values('K')
        fwd=optDataSub.iloc[0]['S']
        stkATM=getATMstk(fwd, optDataSub['K'].unique().tolist())
        up_side = optDataSub[optDataSub['K'] >= stkATM][:3] 
        down_side = optDataSub[optDataSub['K'] < stkATM][-3:]
        opt_df = opt_df.append(up_side)
        opt_df = opt_df.append(down_side)    
    
    indexs = np.random.randint(0,len(opt_df),holding_number)
    portfolios = opt_df.iloc[indexs]
    quantities = np.random.randint(1,10,holding_number)/10
    portfolios['qty'] = quantities

    return portfolios

def get_calibration_contracts(optData):

    optData=optData.sort_values('TTM')
    optExp = pd.Series(optData['EXP_DATE'].values.ravel()).unique()
    opt_df = pd.DataFrame()
    for i in range(0,len(optExp)):
        this_exp_df = pd.DataFrame()
        optDataSub=optData.loc[optData['EXP_DATE'] == optExp[i]]
        optDataSub = optDataSub.sort_values('K')
        fwd=optDataSub.iloc[0]['S']
        stkATM=getATMstk(fwd, optDataSub['K'].unique().tolist())
        otm_call = optDataSub[(optDataSub['K'] >=stkATM) & (optDataSub['cp'] =='C') & (optDataSub['bid_price'] >= 0.01) & (3*optDataSub['bid_price'] > optDataSub['ask_price'])] 
        otm_put = optDataSub[(optDataSub['K'] <= stkATM) & (optDataSub['cp'] =='P') & (optDataSub['bid_price'] >= 0.01) & (3*optDataSub['bid_price'] > optDataSub['ask_price'])] 
        this_exp_df = this_exp_df.append(otm_call)
        this_exp_df = this_exp_df.append(otm_put)    

        if len(this_exp_df)>=5:
            opt_df = opt_df.append(this_exp_df)
 
    return opt_df

def days_diff(exp_date,this_date):
    #exp_date = datetime.strptime("{}".format(exp_date), "%d%b%y")
    exp_date = datetime.strptime("{}".format(exp_date), "%Y%m%d")
    dd = (exp_date-this_date).days
    sd = (exp_date-this_date).seconds/3600/24
    return dd+sd

def symbol2subs(symbol, format_string):
    currency,exp_date,K,cp = symbol.split('-')
    if not currency.endswith("USD"):
        currency = currency+"USD"
    exp_date = datetime.strptime("{}".format(exp_date), format_string)
    exp_date = exp_date.strftime("%Y%m%d")
    instrument = "-".join([currency,exp_date,K,cp])
    subscription = f"Deribit|{instrument}|option|ticker"
    return subscription

def position2instrument(position):
    symbol,contract_type = position.split('_')
    instrument = f"Deribit|{symbol}|{contract_type}"
    return instrument


def update_kdb(table,table_name,logger):
    meta_data = MetaData(**{'qtype': QKEYED_TABLE})
    table = table.reset_index()
    table.set_index(['index'], inplace = True) 
    table.meta = meta_data
    start = datetime.now()
    with qconnection.QConnection(host = 'localhost', port = 5000) as q:
        q.sendSync('{set[`$"this_",x;y];upsert[`$x;y]}',table_name,table)
    #logger.info(f'time used: {datetime.now()-start}')   

def get_agg_positions(positions):
    positions['net_qty'] = positions['qty'] * positions['side']
    pg = positions.groupby('symbol')
    s1=pg['net_qty'].sum()
    s2=pg['group'].first()
    agg_positions = pd.DataFrame([s1,s2]).to_dict()
    return agg_positions

def get_agg_positions_set_ccy(positions):
    positions['settlement_instrument'] = positions['symbol']+"_"+positions['settlement_ccy']
    positions['net_qty'] = positions['qty'] * positions['side']
    positions['amount'] = positions['net_qty'] * positions['avg_price']
    pg = positions.groupby('settlement_instrument')
    s1=pg['net_qty'].sum()
    s2=pg['group'].first()
    s3=pg['settlement_ccy'].first()
    s4=pg['symbol'].first()
    s5=pg['amount'].sum()/s1
    s5.name = 'avg_price'
    agg_positions = pd.DataFrame([s1,s2,s3,s4,s5]).to_dict()
    return agg_positions

def remove_expired_positions(positions):
    is_removed = False
    for key,value in positions.iterrows():
        currency,expiry,strike,cp = value.symbol.split("-")
        exp = datetime.strptime("{}".format(expiry+" 16:00:00"), "%Y%m%d %H:%M:%S")
        dtm = ((exp-datetime.now()).days+(exp-datetime.now()).seconds/3600/24)
        if dtm<=0:
            positions = positions.drop(key)
            is_removed = True
    return positions,is_removed

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)
