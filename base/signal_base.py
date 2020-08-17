class SignalBase:
    def __init__(self, *args):
        self.name = None
        self.value = float('nan')
        self.timestamp = None
        self.parameters = args
        self.subscription_list = []

    def on_market_data_ready_handler(self, data):
        data_type = data['data_type']
        self.timestamp = data['timestamp']
        try:
            getattr(self, f'on_market_data_{data_type}_ready')(data)
            response = {
                'timestamp': self.timestamp,
                'signal_name': self.name,
                'metadata': {
                    'value': self.value,
                }
            }
        except Exception as e:
            response = {
                'timestamp': self.timestamp,
                'signal_name': self.name,
                'metadata': {
                    'value': float('nan'),
                    'error': str(e)
                }
            }
        return response

    def on_market_data_orderbook_ready(self, data):
        raise NotImplementedError("Signal has no implementation for on_market_data_orderbook_ready")

    def on_market_data_trade_ready(self, data):
        raise NotImplementedError("Signal has no implementation for on_market_data_trade_ready")

    def on_market_data_quote_ready(self, data):
        raise NotImplementedError("Signal has no implementation for on_market_data_quote_ready")

    def on_market_data_funding_ready(self, data):
        raise NotImplementedError("Signal has no implementation for on_market_data_funding_ready")

    def on_market_data_index_ready(self, data):
        raise NotImplementedError("Signal has no implementation for on_market_data_index_ready")

    def on_market_data_kline_ready(self, data):
        raise NotImplementedError("Signal has no implementation for on_market_data_kline_ready")

    def on_market_data_quote_ticker_ready(self, data):
        raise NotImplementedError("Signal has no implementation for on_market_data_quote_ticker_ready")

    def on_market_data_bar_ready(self, data):
        raise NotImplementedError("Signal has no implementation for on_market_data_bar_ready")

    def get_value_df(self, date):
        mds = {channel: get_market_data(channel, date) for channel in self.subscription_list}
        return self.from_hist_data(mds)

    def from_hist_data(self, mds):
        raise NotImplementedError("Signal has no implementation for from_hist_data")
