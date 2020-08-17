class IntercomScope:
    Market = 'Md'
    Trade = 'Td'
    MarketRe0 = 'Md0'
    Position = 'Position'
    Risk = 'Risk'
    UI = 'UI'
    Alarm = 'Alarm'
    Console = 'Cnsl'
    Strategy = 'Sc'
    Signal = 'Signal'
    OptionPosition='Opp'


class IntercomChannel:
    SubscribeRequest = 'Subscribe Request'
    SubscribeResponse = 'Subscribe Response'
    UnsubscribeRequest = 'Unsubscribe Request'
    UnsubscribeResponse = 'Unsubscribe Response'
    SendOrderRequest = 'Send Order Request'
    SendOrderResponse = 'Send Order Response'
    CancelOrderRequest = 'Cancel Order Request'
    CancelOrderResponse = 'Cancel Order Response'
    InspectOrderRequest = 'Inspect Order Request'
    InspectOrderResponse = 'Inspect Order Response'
    InquiryBalanceRequest = 'Inquiry Balance Request'
    InquiryBalanceResponse = 'Inquiry Balance Response'
    PollPositionInfoRequest = 'Poll Position Request'
    ORDER_UPDATE_SUBSCRIPTION_REQUEST = "Subscribe Order Update"
    StrategyConfiguration = 'strategy_configuration'
    StrategyConfigurationResponse = 'strategy_configuration_response'
    StrategyConfigUpdate = 'strategy_config_update'
    StrategyConfigResponse = 'strategy_config_response'
    StrategyResponse = 'strategy_response'
    PositionUpdate = 'position_update'
    AutoHedgeUpdate = 'auto_hedge_update'
    TargetUpdate = 'account_target_update'


class OrderActions:
    Send = 'place_order'
    Cancel = 'cancel_order'
    Modify = 'modify_order'
    Inspect = 'inspect_order'
    CancelAll = 'cancel_all_orders'


class RequestActions:
    SendOrder = 'place_order'
    CancelOrder = 'cancel_order'
    InspectOrder = 'inspect_order'
    CancelAllOrder = 'cancel_all_orders'
    QueryPosition = 'query_position'
    QueryBalance = 'query_balance'
    QuerySubaccountBalance = 'query_subaccount_balance'
    QueryOrders = 'query_orders'
    QueryMargin = 'query_margin'
    Deposit = 'deposit'
    Withdraw = 'withdraw'
    QueryHistoryOrders = 'query_history_orders'
    QueryHoldAmount = 'query_hold_amount'
    QueryLatestPrice = 'query_latest_price'
    QueryHistoryTrades = 'query_history_trades'
    QueryWithdrawals = 'query_withdrawals_history'
    QueryDeposits = 'query_deposits_history'
    GetWalletAddress = 'get_wallet_address'
    UpdateComments = 'update_comments'
    ModifyOrder = 'modify_order'
    AddRecord = 'add_record'
    Loan = 'loan'
    TransAsset = 'trans_asset'
    Repay = 'repay'
    QueryLoanHistory = 'query_loan_history'
    Transfer = 'transfer'
    SendOrderBatch = 'place_order_batch'


class OrderStatus:
    Pending = 'pending'
    Submitted = 'new'
    Cancelled = 'cancelled'
    Filled = 'filled'
    PartiallyFilled = 'partially_filled'
    Unknown = 'unknown'


class OrderType:
    Limit = 'Limit'
    Market = 'Market'
    Fak = 'Fak'


class Direction:
    Buy = 'Buy'
    Sell = 'Sell'
    Cover = 'Cover'
    Short = 'Short'


class TradeSide:
    Buy = 'buy'
    Sell = 'sell'


class Status:
    On = 'on'
    Off = 'off'


class PositionInfoType:
    SpotPosition = 'spot_position'
    FuturePosition = 'future_position'
    SpotUserinfo = 'spot_userinfo'
    FutureUserinfo = 'future_userinfo'
    SpotBalance = 'spot_balance'
    FutureBalance = 'future_balance'
    OptionPosition = 'option_position'
    PhysicalSettledFuturePosition = 'physical_settled_future_position'


class ContractType:
    Spot = 'spot'
    FutureThisWeek = 'this_week'
    FutureNextWeek = 'next_week'
    FutureThisQuarter = 'quarter'
    FutureNextQuarter = 'next_quarter'
    FuturePerp = 'perp'
    FutureLTFX = 'lightening_fx'
    FutureIndex = 'index'
    Option = 'option'
    Forex = 'forex'


class MarketData:
    Orderbook = 'orderbook'
    Trade = 'trade'
    Kline = 'kline'
    Price = 'price'
    Quote = 'quote'
    Index = 'index'
    Funding = 'funding'
    Holdamount = 'holdamount'
    Rate = 'rate'
    Liquidation = 'liquidation'
    QuoteTicker = 'quote_ticker'
    QuoteStream = 'quote_stream'


class Event:
    OnLog = 'create_new_log'
    OnError = 'error_occurred'
    OnStopStrategy = 'stop_strategy'
    OnUpdateStrategy = 'update_strategy'
    OnSyncPosition = 'sync_strategy_position'
    OnEnablementChanged = 'enablement_changed'
    OnFeedManagerStart = 'feed_manager_starts'
    OnPositionManagerStart = 'position_manager_starts'
    OnOrderManagerStart = 'order_manager_starts'
    OnSignalManagerStart = 'signal_manager_starts'
