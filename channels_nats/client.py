import asyncio
import typing
from .server import BaseServer
from nats.aio.client import Client as NATS


class ChannelsNATSClient(BaseServer):
    def __init__(self, *, application, verbosity, client_name, loop=None):
        self.application = application
        self.verbosity = verbosity
        self.client_name = client_name
        self.subscriptions = {}

        self.loop = loop or asyncio.get_event_loop()
        self.nc = NATS()

    async def connect(
        self,
        nats: typing.List[str],
        subject: typing.List[str],
        queue: str,
        *args,
        **kwargs
    ):
        """
        Instantiates the connection to the server. Also creates the requisite
        application instance
        """

        scope = {
            "type": "nats",
            "server": nats,
            "client_name": self.client_name,
        }

        # Connect to NATS
        await self.nc.connect(
            servers=nats,
            disconnected_cb=self.disconnected_cb,
            reconnected_cb=self.reconnected_cb,
            error_cb=self.error_cb,
            closed_cb=self.closed_cb,
            loop=self.loop,
        )

        # Subscribe to topics
        for sub in subject:
            application_instance = self.create_application(
                scope=dict(subject=sub, **scope), from_consumer=self.from_consumer(sub)
            )
            subscription = await self.nc.subscribe(
                sub, cb=self.from_nats(sub), queue=queue
            )
            application_instance.subscription = subscription

            self.subscriptions[sub] = application_instance

        self.loop.call_later(1, self.futures_checker)

    def start(self):
        self.loop.run_forever()

    async def disconnect(self):
        """
        Disconnects from the current NATS connection
        """

        await self.nc.drain()

    def from_consumer(self, subscriber):
        """
        Receives message from channels from the consumer. Message should have the format:
            {
                'type': 'nats.send',
                'command': VALID_COMMAND_TYPE
            }
        """

        async def handle(message):
            if "type" not in message:
                raise ValueError("Message has no type defined")

            elif message["type"] == "nats.send":
                await self.nc.publish(
                    subject=message["subject"], payload=message["bytes"],
                )

            else:
                raise ValueError("Cannot handle message type %s!" % message["type"])

        return handle

    def from_nats(self, subscriber):
        async def message_handler(msg):
            application_instance = self.subscriptions[subscriber]

            await self._send_application_msg(
                application_instance,
                {
                    "type": "nats.receive",
                    "subject": msg.subject,
                    "bytes": msg.data,
                    "reply": msg.reply,
                },
            )

        return message_handler

    #
    # NATS callbacks
    #

    async def disconnected_cb(self):
        print("[disconnected]")

    async def reconnected_cb(self):
        print("[reconnected]")

    async def error_cb(self, exc):
        print("[error]", exc)

    async def closed_cb(self):
        print("[closed]")
