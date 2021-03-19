import logging
from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Callable, List
from time import time


def check_risk(method: Callable):
    @wraps(method)
    def wrapper(*args, **kwargs):
        method(*args, **kwargs)
        risk, ts = args[:2]
        risk.update_time = ts
        return risk.value_check()

    return wrapper


class AlarmLevel(str, Enum):
    WOLF = '狼'  # 灾害等级：狼
    TIGER = '虎'  # 灾害等级：虎
    DEMON = '鬼'  # 灾害等级：鬼
    DRAGON = '龙'  # 灾害等级：龙
    GOD = '神'  # 灾害等级：神


@dataclass
class Alarm:
    level: AlarmLevel
    message: str


class Action(str, Enum):
    INVALID = 'invalid'
    QUERY = 'query_orders'
    INSPECT = 'inspect_order'
    PLACE = 'place_order'
    CANCEL = 'cancel_order'
    STRATEGY = 'strategy'
    PRICE = 'price'


@dataclass
class Event:
    event_time: float = field(default=time(), init=False)
    action: Action = field(default=Action.INVALID, init=False)


@dataclass
class PriceEvent(Event):
    price: float
    exchange_time: float
    action = Action.PRICE


class RiskBase(ABC):
    def __init__(self, life: int):
        self.update_time = 0
        self.life: int = life
        self.name = type(self).__name__

    def on_time(self, ts) -> Alarm:
        if (t := ts - self.update_time) > self.life:
            return Alarm(AlarmLevel.WOLF, f'Not updated for {t} seconds')

    def on_event(self, event: Event) -> Alarm:
        self.update_time = event.event_time
        getattr(self, event.action.lower())(event)

    def on_price(self, e: PriceEvent) -> Alarm:
        """ Not implemented """

    def on_position(self, e: PositionEvent) -> Alarm:
        """ Not implemented """

    def on_send_order(self, e: SendOrderEvent) -> Alarm:
        """ Not implemented """

    def on_inspect_order(self, e: InspectOrderEvent) -> Alarm:
        """ Not implemented """

    def on_cancel_order(self, e: CancelOrderEvent) -> Alarm:
        """ Not implemented """


class RiskHub:
    def __init__(self, strategy_name: str):
        self.indicators: List[RiskBase] = list()
        self.logger = logging.getLogger(f'RiskHub<{strategy_name}>')

    def send_alarm(self, alarm: Alarm):
        self.logger.warning(alarm.level.value, alarm.message)

    def add_indicator(self, indicator: RiskBase) -> None:
        assert isinstance(indicator, RiskBase), TypeError(f'Invalid risk indicator: {indicator=}')
        if indicator not in self.indicators:
            self.indicators.append(indicator)

    def emit_event(self, event: Event) -> None:
        for indicator in self.indicators:
            if alarm := indicator.on_event(event):
                self.send_alarm(alarm)

    def on_position(self, data: dict) -> None:
        self.logger.debug(data)

    def on_md(self, data: dict) -> None:
        self.logger.debug(data)
        event = PriceEvent(price=100)
        self.emit_event(event)

    def on_td(self, data: dict) -> None:
        self.logger.debug(data)
