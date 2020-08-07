import requests
import pandas as pd
import numpy as np
from pathlib2 import Path, PosixPath

from configs import CONFIGS
from configs.typedef import MarketData, TradeSide, ContractType
from library import get_signal


def _get_npz_path(exchange: str, symbol: str, contract_type: str, data_type: str, date: str,
                  location: str = 'hk') -> PosixPath:
    """
    获取数据文件路径
    """
    file_name = f'{location}-{exchange}-{symbol}-{contract_type}-{data_type}-{date}.npz'.lower()
    file_path = Path(CONFIGS['directions']['md']) / location / date / exchange.lower() / file_name
    return file_path


def _get_market_data_dict_from_file(exchange: str, symbol: str, contract_type: str, data_type: str, date: str,
                                    location: str = 'hk') -> dict:
    """
    从文件读取数据
    """
    npz_path = _get_npz_path(exchange, symbol, contract_type, data_type, date, location)
    if npz_path.is_file():
        with np.load(str(npz_path), 'r') as f:
            data = dict(f)
        if data_type == MarketData.Orderbook:
            data['asks'][data['asks'] == 0] = float('nan')
            data['bids'][data['bids'] == 0] = float('nan')
        return dict(data)
    else:
        raise FileExistsError('npz file does not exist.')


def _get_market_data_dict_from_api(exchange: str, symbol: str, contract_type: str, data_type: str, date: str,
                                   location: str = 'hk') -> dict:
    """
    从api读取数据
    """
    url = CONFIGS['urls'][
              'md'] + f'/{date}/{exchange}?symbol={symbol}&type={contract_type}&datatype={data_type}'.lower()
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()['data']
        return data
    else:
        raise ConnectionError(f'Error code: {response.status_code}')


def get_market_data_dict(exchange: str, symbol: str, contract_type: str, data_type: str, date: str,
                         location: str = 'hk'):
    try:
        data = _get_market_data_dict_from_file(exchange, symbol, contract_type, data_type, date, location)
    except FileExistsError:
        data = _get_market_data_dict_from_api(exchange, symbol, contract_type, data_type, date, location)
    return data


def _get_raw_market_data(exchange: str, symbol: str, contract_type: str, data_type: str, date: str,
                         location: str = 'hk') -> pd.DataFrame:
    data = get_market_data_dict(exchange, symbol, contract_type, data_type, date, location)
    df = pd.DataFrame(zip(*data.values()), columns=data.keys())
    for col in df.columns:
        if 'time' in col:
            df[col] = pd.to_datetime(df[col])
    df = df.sort_values('local_timestamp')
    return df


def _get_aggregated_market_data(exchange: str, symbol: str, contract_type: str, freq: str, date: str,
                                location: str = 'hk') -> pd.DataFrame:
    td_df = _get_raw_market_data(exchange, symbol, contract_type, MarketData.Trade, date, location).copy()
    ob_df = _get_raw_market_data(exchange, symbol, contract_type, MarketData.Orderbook, date, location).copy()
    timestamp = pd.date_range(start=date, periods=pd.Timedelta('1d') // pd.Timedelta(freq) + 1, closed='right',
                              freq=freq)
    td_df = td_df.copy()
    td_df['volume'] = td_df['size_in_coin'] if 'size_in_coin' in td_df.columns else td_df['size']
    td_df['buy_volume'] = td_df['volume'].where(td_df['side'] == TradeSide.Buy, other=0)
    td_df['sell_volume'] = td_df['volume'].where(td_df['side'] == TradeSide.Sell, other=0)
    td_df['local_timestamp'] = td_df['local_timestamp'].dt.round(freq)
    ob_df['mid_price'] = (ob_df['asks'].apply(lambda _: _[0][0]) + ob_df['bids'].apply(lambda _: _[0][0])) * 0.5

    grp = td_df.groupby('local_timestamp')
    price_df = pd.DataFrame()
    price_df['open'] = grp['price'].first()
    price_df['high'] = grp['price'].max()
    price_df['low'] = grp['price'].min()
    price_df['close'] = grp['price'].last()
    price_df = price_df.reindex(timestamp)
    price_df['close'] = price_df['close'].fillna(method='ffill')
    for _ in ['open', 'high', 'low']:
        price_df[_] = np.where(np.isnan(price_df[_]), price_df['close'], price_df[_])
    volume_df = pd.DataFrame()
    volume_df['volume'] = grp['volume'].sum()
    volume_df['buy_volume'] = grp['buy_volume'].sum()
    volume_df['sell_volume'] = grp['sell_volume'].sum()
    volume_df = volume_df.reindex(timestamp)
    volume_df = volume_df.fillna(0)
    bar_df = pd.concat([price_df, volume_df], axis=1)
    bar_df.index = range(bar_df.shape[0])
    bar_df['local_timestamp'] = timestamp
    bar_df = pd.merge_asof(bar_df, ob_df, on='local_timestamp').fillna(method='ffill')
    return bar_df


def get_market_data(channel: str, date: str, location: str = 'hk') -> pd.DataFrame:
    exchange, symbol, contract_type, data_type = channel.split('|')
    if data_type[0].isdigit():
        md = _get_aggregated_market_data(exchange, symbol, contract_type, data_type, date, location)
    else:
        md = _get_raw_market_data(exchange, symbol, contract_type, data_type, date, location)
    return md


def get_signal_data(signal_name: str, date: str, location: str = 'hk') -> pd.DataFrame:
    signal = get_signal(signal_name)
    mds = {_: get_market_data(_, date, location) for _ in signal.subscribe_channels}
    df = signal.from_hist_data(mds).dropna(axis=0)
    return df
