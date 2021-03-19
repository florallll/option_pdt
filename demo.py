import json

import aioredis

from pdt_riskmonitor.risk import RiskBase, RiskHub, Alarm, AlarmLevel, check_risk


class Beta(RiskBase):

    def __init__(self, life: int, lower: float, upper: float):
        super().__init__(life)
        self.lower = lower
        self.upper = upper
        self.order_queue: list = list()

    def value_check(self) -> Alarm:
        if self.value < self.lower:
            return Alarm(AlarmLevel.TIGER, "Beta is under lower bound")
        if self.value > self.upper:
            return Alarm(AlarmLevel.TIGER, "Beta is above upper bound")

    @check_risk
    def on_position(self, ts: float, value: float) -> None:
        self.value = value


class Temp(RiskBase):
    def __init__(self, life: int, upper_bound: float):
        super().__init__(life)
        self.value = 0

    @check_risk
    def on_send_order(self, ts: float, data: dict) -> None:
        pass

    @check_risk
    def on_cancel_order(self, ts: float, data: dict) -> None:
        pass

    @check_risk
    def on_inspect_order(self, ts: float, data: dict) -> None:
        pass


class Greek(RiskBase):
    def __init__(self, life: int, bounds: float):
        super().__init__(life)
        self.local_vega: float = float('nan')
        self.exchange_vega: float = float('nan')

    def value_check(self) -> Alarm:
        if abs(self.local_vega - self.exchange_vega) >= 100:
            return Alarm()

    def on_position(self, ts: float, data: dict) -> None:
        pass

    def on_strategy(self, ts: float, data: dict) -> None:
        pass


class MaxDrawDown(RiskBase):
    def __init__(self, life: int, thd: float):
        super(MaxDrawDown, self).__init__(life)
        self.max_pnl = float('nan')
        self.thd = thd

    def on_price(self, ts: float, value: float) -> None:
        pass

    def on_position(self, ts: float, value: float) -> None:
        pass

    @check_risk
    def on_pnl(self, ts: float, value: float) -> None:
        self.max_pnl = max(value, self.max_pnl)
        self.value = value / self.max_pnl - 1


async def main(strategy_name: str, exchange: str, account_id: str):
    rh = RiskHub('demo_stg')
    rh.add_indicator(Beta(10, 0, 5))
    redis = await aioredis.create_redis('redis://:KzunGdlIa1S3OLxk!@10.80.20.149:6379/0?encoding=utf-8')
    receiver = aioredis.pubsub.Receiver()
    await redis.psubscribe(receiver.pattern(f'Position:{exchange}|{account_id}'), receiver.pattern('Td:*'),
                           receiver.pattern('Md:*'))
    async for ch, msg in receiver.iter(encoding='utf-8', decoder=json.loads):
        scope = ch.name.decode().split(":")[0]
        if handler := getattr(rh, f'on_{scope.lower()}'):
            handler(msg)


if __name__ == '__main__':
    import asyncio
    import logging

    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
