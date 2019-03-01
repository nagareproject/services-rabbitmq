# --
# Copyright (c) 2008-2019 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

import time

from nagare.admin import command


class Command(command.Command):
    WITH_STARTED_SERVICES = True

    def run_with_channel(self, channel, rabbitmq_service, services_service, **config):
        try:
            channel = services_service[channel]
            return self.run(rabbitmq_service, channel, **config)
        except KeyboardInterrupt:
            return 0

    def _run(self, command_names, **params):
        return super(Command, self)._run(command_names, self.run_with_channel, **params)


class Receive(Command):
    DESC = 'Receive data on a RabbitMQ queue'

    def __init__(self, name, dist, **config):
        super(Receive, self).__init__(name, dist, **config)
        self.nb = 0

    def set_arguments(self, parser):
        parser.add_argument('channel', help='')

        super(Command, self).set_arguments(parser)

    def handle_request(self, msg):
        print(('- %d ' % self.nb) + ('-' * 20))

        print('Body: ' + msg.body.decode('utf-8'))
        print('')

        del msg.delivery_info['channel']

        print('Delivery info:')
        padding = len(max(msg.delivery_info, key=len)) if msg.properties else ''
        for k, v in sorted(msg.delivery_info.items()):
            print(' - %s: %s' % (k.ljust(padding), v))
        print('')

        print('Properties:')
        padding = len(max(msg.properties, key=len)) if msg.properties else ''
        for k, v in sorted(msg.properties.items()):
            print(' - %s: %s' % (k.ljust(padding), v))

        self.nb += 1
        print('')

    def run(self, rabbitmq, channel, **config):
        print(
            'Listening on queue <{}>, exchange <{}>, route <{}>...'.format(
                channel.queue,
                channel.exchange,
                channel.route
            )
        )
        channel.on_receive(self.handle_request)
        rabbitmq.start()

        return 0


class Send(Command):
    DESC = 'Send data on a RabbitMQ exchange'

    def set_arguments(self, parser):
        parser.add_argument(
            '-l', '--loop', action='store_true',
            help=''
        )

        parser.add_argument('channel', help='')
        parser.add_argument('data', help='')

        super(Send, self).set_arguments(parser)

    @staticmethod
    def run(rabbitmq, channel, loop, data):
        print(
            'Sending on queue <{}>, exchange <{}>, route <{}>'.format(
                channel.queue,
                channel.exchange,
                channel.route
            )
        )
        while True:
            channel.send_raw(data)

            if not loop:
                break

            time.sleep(2)

        return 0
