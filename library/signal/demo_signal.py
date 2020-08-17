from base.signal_base import SignalBase


class Signal(SignalBase):

    def __init__(self, spot_instrument, future_instrument):
        super().__init__(spot_instrument, future_instrument)
        self.spot_instrument = spot_instrument
        self.future_instrument = future_instrument
        self.spot_price = float('nan')
        self.future_price = float('nan')
        self.subscription_list = [f'{_}|1s' for _ in [spot_instrument, future_instrument]]

    def on_market_data_1s_ready(self, data):
        instrument = '|'.join([data['exchange', 'symbol', 'contract_type']])
        if instrument == self.spot_instrument:
            self.spot_price = data['metadata']['mid']
        else:
            self.future_price = data['metadata']['mid']
        self.value = self.future_price / self.spot_price - 1

    def from_hist_data(self, mds):
        spot_orderbook = mds[self.subscription_list[0]]
        fut_orderbook = mds[self.subscription_list[1]]
        df = spot_orderbook[['local_timestamp']].copy()
        df['value'] = fut_orderbook['mid'] / spot_orderbook['mid'] - 1
        return df
