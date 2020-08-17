import json
from threading import Thread
from redis import ConnectionPool, Redis
from configs import REDIS_CONFIGS

_CONNECTION_POOL = {}


def _get_redis_conn(redis_name):
    if redis_name not in _CONNECTION_POOL:
        _CONNECTION_POOL[redis_name] = ConnectionPool(**REDIS_CONFIGS[redis_name])
    conn = Redis(connection_pool=_CONNECTION_POOL[redis_name])
    return conn


class Intercom:
    def __init__(self):
        self.connections = {'localhost': _get_redis_conn('localhost')}

    def emit(self, scope, channel, data, redis_name='localhost'):
        if redis_name not in self.connections:
            self.connections[redis_name] = _get_redis_conn(redis_name)
        Thread(target=self.connections[redis_name].publish, args=(f'{scope}:{channel}', json.dumps(data),)).start()

    def subscribe(self, pattern, redis_name='localhost'):
        pubsub = self.connections[redis_name].pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(pattern)
        return pubsub
