# Encoding: utf-8

# --
# Copyright (c) 2008-2019 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

""" Provides the classes to interact with RabbitMQ """

from functools import partial
from datetime import datetime

import pika

from nagare.services import plugin


CONNECTION_PARAMS = (
    ('backpressure_detection', 'boolean'),
    ('blocked_connection_timeout', 'float'),
    ('channel_max', 'integer'),
    ('connection_attempts', 'integer'),
    ('frame_max', 'integer'),
    ('heartbeat', 'integer'),
    ('host', 'string'),
    ('locale', 'string'),
    ('port', 'integer'),
    ('retry_delay', 'float'),
    ('socket_timeout', 'float'),
    # ('ssl', 'boolean'),
    ('virtual_host', 'string'),
    ('tcp_options', 'integer')
)


def create_config_spec():
    parameters = pika.connection.Parameters()
    spec = {name: '{}(default={})'.format(type_, getattr(parameters, name)) for name, type_ in CONNECTION_PARAMS}

    spec.update({
        'user': 'string(default="")',
        'password': 'string(default="guest")',
        'pool': 'integer(default=0)'
    })

    return spec


class Message(object):

    def __init__(self, body, delivery_info, properties):
        self.body = body
        self.delivery_info = delivery_info
        self.properties = properties

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
    CONFIG_SPEC = create_config_spec()

    def __init__(self, name, dist, user, password, pool, **config):
        super(RabbitMQ, self).__init__(name, dist, **config)

        self.user = user
        self.password = password
        self.pool = pool
        self.started = False
        self.connection = None

    @property
    def is_open(self):
        return (self.connection is not None) and self.connection.is_open

    @property
    def is_closed(self):
        return (self.connection is None) or self.connection.is_closed

    def handle_start(self, app):
        '''
        pool = self.plugin_config.pop('pool')
        if pool is not None:
            from gevent import sleep, pool

            pool = pool.Pool(pool)
            pool.spawn(sleep, .5)  # Workaround for the `haigha` bug where an empty pool is discarded

        self.connection = Connection(
            sock_opts={(socket.IPPROTO_TCP, socket.TCP_NODELAY): 1},
            close_cb=self.on_close, pool=pool,
            **self.plugin_config
        )
        '''
        config = {k: v for k, v in self.plugin_config.items() if k in CONNECTION_PARAMS}
        if self.user:
            config['credentials'] = pika.credentials.PlainCredentials(self.user, self.password)

        parameters = pika.connection.ConnectionParameters(**config)
        self.connection = pika.BlockingConnection(parameters)

    def close(self, reply_code=200, reply_text='Normal shutdown'):
        self.connection.close(reply_code, reply_text)

    def channel(self, channel_id=None):
        """Fetch or create a channel on the connection
        """
        return self.connection.channel(channel_id)

    def create_channel(self, prefetch=None):
        """Create a channel on the connection
        """
        channel = self.channel()
        if prefetch:
            channel.basic_qos(prefetch_count=prefetch)

        return channel

    def send_heartbeat(self):
        heartbeat = pika.heartbeat.heartbeatChecker(self.connection, self.plugin_config['heartbeat'])
        heartbeat._send.heartbeat_frame()

    def start(self):
        """Start the rabbitmq connection.
        """

        self.started = True

        try:
            while self.started:
                self.connection.process_data_events(None)
        except KeyboardInterrupt:
            self.started = False

        self.close()

    def stop(self):
        was_started, self.started = self.started, False
        return was_started
    handle_stop = stop


class Channel(plugin.Plugin):
    LOAD_PRIORITY = 15
    CONFIG_SPEC = {
        'exchange': 'string',
        'mode': 'string(default="direct")',
        'queue': 'string',
        'route': 'string(default="")',
        'auto_delete': 'boolean(default=True)',
        'durable': 'boolean(default=False)',
        'pool': 'integer(default=None)',
        'prefetch': 'integer(default=None)'
    }

    def __init__(
            self,
            name, dist,
            exchange, queue, rabbitmq_service,
            route='', durable=False, prefetch=None,
            **config
    ):
        super(Channel, self).__init__(name, dist)

        self.rabbitmq = rabbitmq_service
        self.exchange = exchange
        self.queue = queue
        self.route = route
        self.durable = durable
        self.prefetch = prefetch

        self.out_channel = self.in_channel = None
        self.semaphore = None

    def _handle_start(
            self,
            queue, auto_delete, durable,
            route,
            exchange, mode,
            pool, prefetch
    ):
        '''
        if not pool:
            self.semaphore = None
            self.out_channel = self.rabbitmq.create_channel()
        else:
            from gevent.lock import Semaphore

            self.semaphore = Semaphore(pool)
            self.out_channel = {self.rabbitmq.create_channel() for _ in range(pool)}
        '''
        self.semaphore = None
        self.out_channel = self.rabbitmq.create_channel()

        channel = self.rabbitmq.create_channel(prefetch=prefetch)
        channel.exchange_declare(exchange, mode)
        channel.queue_declare(queue, auto_delete=auto_delete, durable=durable)
        channel.queue_bind(queue, exchange, route)

        self.in_channel = channel

    def handle_start(self, app):
        self._handle_start(**self.plugin_config)

    def handle_stop(self, reply_code=0, reply_text='Normal shutdown'):
        if self.semaphore:
            for channel in self.out_channel:
                channel.close(reply_code, reply_text)
        else:
            self.out_channel.close(reply_code, reply_text)

        self.out_channel = None

        self.in_channel.close(reply_code, reply_text)
        self.in_channel = None

    def acquire_out_channel(self):
        if self.semaphore:
            self.semaphore.acquire()
            channel = self.out_channel.pop()
        else:
            channel = self.out_channel

        return channel

    def release_out_channel(self, channel):
        if self.semaphore:
            self.out_channel.add(channel)
            self.semaphore.release()

    def send_raw(self, msg, mandatory=False, immediate=False, **properties):
        properties['delivery_mode'] = 2 if self.durable else 1

        channel = self.acquire_out_channel()
        try:
            channel.basic_publish(
                self.exchange, self.route,
                msg, pika.spec.BasicProperties(**properties),
                mandatory=mandatory, immediate=immediate
            )
        finally:
            self.release_out_channel(channel)

    def send(self, correlation_id, app_id, type_, msg, **properties):
        self.send_raw(
            msg,
            type=type_,
            timestamp=datetime.utcnow(),
            correlation_id=correlation_id,
            app_id=app_id,
            **properties
        )

    @staticmethod
    def _on_receive(consumer, channel, method_frame, properties, body):
        message = Message(
            body,
            dict(method_frame.__dict__, channel=channel),
            {k: v for k, v in properties.__dict__.items() if v is not None}
        )

        consumer(message)

    def on_receive(self, consumer, exclusive=False, consumer_tag=None):
        self.in_channel.basic_consume(
            partial(self._on_receive, consumer), self.queue,
            no_ack=not self.prefetch,
            exclusive=exclusive,
            consumer_tag=consumer_tag
        )

    def ack(self, message, multiple=False):
        if self.prefetch is not None:
            delivery_tag = message.delivery_info['delivery_tag']
            self.in_channel.basic_ack(delivery_tag, multiple)

    def reject(self, message, multiple=False, requeue=False):
        if self.prefetch is not None:
            delivery_tag = message.delivery_info['delivery_tag']
            self.in_channel.basic_nack(delivery_tag, multiple=multiple, requeue=requeue)
