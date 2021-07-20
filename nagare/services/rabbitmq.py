# Encoding: utf-8

# --
# Copyright (c) 2008-2021 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

"""Provides the classes to interact with RabbitMQ"""

from functools import partial
from concurrent.futures import ThreadPoolExecutor
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

import amqpstorm
import transaction

from nagare.services import plugin, proxy


class Message(amqpstorm.message.Message):

    @property
    def delivery_info(self):
        return dict(self.method, channel=self.channel)

    def __str__(self):
        return 'Message[body: {}, delivery_info: {},  properties: {}]'.format(
            self.body,
            self.delivery_info,
            self.properties
        )


class RabbitMQ(plugin.Plugin):
    """The RabbitMQ client service
    """
    LOAD_PRIORITY = 10
    CONFIG_SPEC = dict(
        plugin.Plugin.CONFIG_SPEC,
        uri='string(default=None)',
        host='string(default="127.0.0.1")',
        port='integer(default=5672)',
        user='string(default="guest")',
        password='string(default="guest")',
        vhost='string(default="/")',
        connect_timeout='float(default=0)',
        heartbeat='integer(default=60)',
        lazy='boolean(default=False)',
        ssl='boolean(default=False)'
    )
    CONFIG_TRANSLATIONS = {
        'host': 'hostname',
        'user': 'username',
        'vhost': 'virtual_host',
        'connect_timeout': 'timeout'
    }

    def __init__(self, name, dist, **config):
        super(RabbitMQ, self).__init__(name, dist, **config)
        self.connection = None

    @property
    def is_open(self):
        return (self.connection is not None) and self.connection.is_open

    @property
    def is_closed(self):
        return (self.connection is None) or self.connection.is_closed

    def open(self):
        self.connection.open()

    def close(self):
        self.connection.close()

    def handle_start(self, app):
        config = {self.CONFIG_TRANSLATIONS.get(k, k): v for k, v in self.plugin_config.items()}

        uri = config.pop('uri')
        if uri:
            uri = urlparse.urlparse(uri)
            if uri.scheme == 'amqp':
                config.update({
                    'hostname': uri.hostname or '127.0.0.1',
                    'port': uri.port or 5672,
                    'username': uri.username or 'guest',
                    'password': uri.password or 'guest',
                    'virtual_host': uri.path or '/'
                })

        self.connection = amqpstorm.Connection(**config)

    def create_channel(self, prefetch=None):
        """Create a channel on the connection
        """
        channel = self.connection.channel()
        if prefetch:
            channel.basic.qos(prefetch)

        return channel

    def send_heartbeat(self):
        self._channel0.send_heartbeat()


class _Channel(object):
    def __init__(
            self,
            rabbitmq_service, exchange=None, queue=None,
            route='', durable=False, prefetch=None,
            auto_decode=False, pool=1, transaction=True,
            **config
    ):
        self.rabbitmq = rabbitmq_service
        self.exchange = exchange
        self.queue = queue
        self.route = route
        self.durable = durable
        self.prefetch = prefetch
        self.auto_decode = auto_decode
        self.pool_size = pool
        self.transaction = transaction

        self.pool = None
        self.out_channel = self.in_channel = None

    def close(self, reply_code=0, reply_text='Normal shutdown'):
        self.out_channel.close(reply_code, reply_text)
        self.out_channel = None

        self.in_channel.close(reply_code, reply_text)
        self.in_channel = None

    def create_out_channel(self, **params):
        return self.rabbitmq.create_channel(**params)

    def create_in_channel(self, **params):
        return self.rabbitmq.create_channel(**params)

    @staticmethod
    def declare_in_exchange(channel, exchange, mode, **params):
        channel.exchange.declare(exchange, mode, **params)

    @staticmethod
    def declare_in_queue(channel, queue, **params):
        channel.queue.declare(queue, **params)

    def handle_start(
            self,
            queue, auto_delete, durable,
            route,
            exchange, mode,
            prefetch, **config
    ):
        self.out_channel = self.create_out_channel()

        in_channel = self.create_in_channel(prefetch=prefetch)

        if exchange is not None:
            self.declare_in_exchange(in_channel, exchange, mode)

        if queue is not None:
            self.declare_in_queue(in_channel, queue, auto_delete=auto_delete, durable=durable)

        if (queue is not None) and (exchange is not None):
            in_channel.queue.bind(queue, exchange, route)

        self.in_channel = in_channel

    def handle_request(self, chain, **params):
        if self.transaction:
            self.out_channel.tx.select()
            transaction.get().join(self)

        return chain.next(**params)

    def sortKey(self):
        return '~sqlz'  # Commit after the SQLAlchemy transaction

    def tpc_finish(self, transaction):
        self.out_channel.tx.commit()

    def abort(self, transaction):
        self.out_channel.tx.rollback()

    tpc_abort = abort
    tpc_begin = tpc_commit = tpc_vote = commit = lambda self, transaction: None

    def send_raw_message(self, message, mandatory=False, immediate=False):
        if self.exchange is not None:
            message.delivery_mode = 2 if self.durable else 1
            message.publish(self.route, self.exchange, mandatory=mandatory, immediate=immediate)

    def send_raw(self, body, mandatory=False, immediate=False, **properties):
        message = Message(self.out_channel, body=body, properties=properties)
        self.send_raw_message(message, mandatory, immediate)

    def send(self, correlation_id, app_id, content_type, body, mandatory=False, immediate=False, **properties):
        properties.update({
            'correlation_id': correlation_id,
            'app_id': app_id,
            'content_type': content_type
        })
        message = Message.create(self.out_channel, body, properties)
        self.send_raw_message(message, mandatory, immediate)

    def _on_receive(self, consumer, body, channel, method, properties):
        message = Message(channel, body=body, method=method, properties=properties, auto_decode=self.auto_decode)
        self.pool.submit(consumer, message).add_done_callback(lambda future: future.result())

    def on_receive(self, consumer, exclusive=False, consumer_tag=''):
        self.in_channel.basic.consume(
            partial(self._on_receive, consumer), self.queue,
            no_ack=not self.prefetch,
            exclusive=exclusive,
            consumer_tag=consumer_tag
        )

    def start_consuming(self):
        if self.pool is None:
            self.pool = ThreadPoolExecutor(self.pool_size)

        self.in_channel.start_consuming(to_tuple=True)

    def stop_consuming(self):
        self.in_channel.stop_consuming()
        self.pool.shutdown()
        self.pool = None

    def ack(self, message, multiple=False):
        if self.prefetch is not None:
            delivery_tag = message.delivery_info['delivery_tag']
            self.in_channel.basic.ack(delivery_tag, multiple)

    def nack(self, message, multiple=False, requeue=False):
        if self.prefetch is not None:
            delivery_tag = message.delivery_info['delivery_tag']
            self.in_channel.basic.nack(delivery_tag, multiple=multiple, requeue=requeue)

    def reject(self, message, multiple=False, requeue=False):
        if self.prefetch is not None:
            delivery_tag = message.delivery_info['delivery_tag']
            self.in_channel.basic.reject(delivery_tag, multiple=multiple, requeue=requeue)


@proxy.proxy_to(_Channel, lambda self: self.channels[self.name], {'handle_start'})
class Channel(plugin.Plugin):
    # LOAD_PRIORITY = 15
    CONFIG_SPEC = dict(
        plugin.Plugin.CONFIG_SPEC,
        exchange='string(default=None)',
        mode='string(default="direct")',
        queue='string(default=None)',
        route='string(default="")',
        auto_delete='boolean(default=True)',
        durable='boolean(default=False)',
        prefetch='integer(default=None)',
        auto_decode='boolean(default=False)',
        pool='integer(default=1)',
        transaction='boolean(default=True)'
    )
    channels = {}

    def __init__(
            self,
            name, dist,
            rabbitmq_service, exchange=None, queue=None,
            mode='direct', route='', auto_delete=True, durable=False, prefetch=None,
            auto_decode=False, pool=1, transaction=True,
            services_service=None,
            **config
    ):
        services_service(
            super(Channel, self).__init__, name, dist,
            exchange=exchange, queue=queue,
            mode=mode, route=route, auto_delete=auto_delete, durable=durable, prefetch=prefetch,
            auto_decode=auto_decode, pool=pool, transaction=transaction,
            **config
        )

        self.queue = queue
        self.exchange = exchange
        self.route = route

        self.__class__.channels[name] = _Channel(
            rabbitmq_service, exchange=exchange, queue=queue,
            mode=mode, route=route, auto_delete=auto_delete, durable=durable, prefetch=prefetch,
            auto_decode=auto_decode, pool=pool, transaction=transaction,
            **config
        )

    def handle_start(self, app):
        self.channels[self.name].handle_start(**self.plugin_config)
