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


class Commands(command.Commands):
    DESC = 'RabbitMQ messaging subcommands'


class Command(command.Command):
    WITH_STARTED_SERVICES = True

    def run_with_channel(self, channel, rabbitmq_service, services_service, **config):
        channel = services_service[channel]

        status = self.run(rabbitmq_service, channel, **config)

        channel.close()
        rabbitmq_service.close()

        return status

    def _run(self, command_names, **params):
        return super(Command, self)._run(command_names, self.run_with_channel, **params)


class Receive(Command):
    DESC = 'receive data on a RabbitMQ queue'

    def __init__(self, name, dist, **config):
        super(Receive, self).__init__(name, dist, **config)
        self.nb = 0

    def set_arguments(self, parser):
        parser.add_argument('channel', help='name of the channel service to receive from')

        super(Receive, self).set_arguments(parser)

    def handle_request(self, msg):
        print('- {} --------------------'.format(self.nb))

        print('Body: {}'.format(msg.body))
        print('')

        delivery_info = msg.delivery_info
        del delivery_info['channel']

        print('Delivery info:')
        padding = len(max(delivery_info, key=len)) if msg.properties else ''
        for k, v in sorted(delivery_info.items()):
            print(' - {}: {}'.format(k.ljust(padding), v))
        print('')

        print('Properties:')
        padding = len(max(msg.properties, key=len)) if msg.properties else ''
        for k, v in sorted(msg.properties.items()):
            print(' - {}: {}'.format(k.ljust(padding), v))

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

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        return 0


class Send(Command):
    DESC = 'send data on a RabbitMQ exchange'

    def set_arguments(self, parser):
        parser.add_argument(
            '-l', '--loop', action='store_true',
            help='infinite loop sending <data> each 2 secondes'
        )

        parser.add_argument('channel', help='name of the channel service to send to')
        parser.add_argument('data', help='string to send')

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
        try:
            while True:
                channel.send_raw(data)

                if not loop:
                    break

                time.sleep(1)
        except KeyboardInterrupt:
            pass

        return 0
