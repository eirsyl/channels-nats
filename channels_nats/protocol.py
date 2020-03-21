import typing
import logging
import asyncio
from nats.aio.errors import ErrTimeout
from functools import partial

if typing.TYPE_CHECKING:
    from .client import Client

logger = logging.getLogger(__name__)


class NATSProtocol:
    def __init__(self, client: "Client", subject: str) -> None:
        """
        Initialize the protocol with the required properties.
        """

        self.client: "Client" = client
        self.subject: str = subject

        self.application_queue: typing.Optional[asyncio.Queue] = None
        self.sid: typing.Optional[int] = None

        task = self.client.loop.create_task(self.setup())
        task.add_done_callback(self.setup_complete)

    #
    # Setup
    #

    async def setup(self) -> None:
        # Setup the NATS subscription used to listen on NATS messages.
        self.sid = await self.client.nats.subscribe(
            subject=self.subject, queue="worker", cb=self.handle_message
        )
        self.client.protocol_connected(self)

        # Spin up the asgi app and initialize the application queue.
        self.application_queue = self.client.create_application(
            self, {"type": "nats", "subject": self.subject}
        )

        # Send the connect event to the ASGI layer
        await self.handle_connect()

    def setup_complete(self, future: asyncio.Future) -> None:
        try:
            future.result()
        except Exception:
            logger.exception("Failed to setup protocol application")

    async def disconnect(self) -> None:
        # Send the disconnect event to the ASGI layer
        await self.handle_disconnect()

        # Drain application queue
        if self.application_queue:
            await self.application_queue.join()

        # Unsubscribe from the NATS topic
        await self.client.nats.unsubscribe(self.sid)
        self.client.protocol_disconnected(self)

    def disconnect_complete(self, future: asyncio.Future) -> None:
        try:
            future.result()
        except Exception:
            logger.exception("Failed to disconnect from NATS")

    def handle_exception(self, exception: Exception) -> None:
        task = self.client.loop.create_task(self.disconnect())
        task.add_done_callback(self.disconnect_complete)

    async def handle_reply(self, message: typing.Any) -> None:
        """
        Reply from the channels consumer.
        """

        if "type" not in message:
            raise ValueError("Message has no type defined")

        if "subject" not in message:
            raise ValueError("Message has no subject defined")

        if message["type"] == "nats.send":
            await self.client.nats.publish(
                subject=message["subject"], payload=message.get("bytes_data", None)
            )

        elif message["type"] == "nats.request":
            if "timeout" not in message:
                raise ValueError("Message has no timeout defined")

            try:
                response = await self.client.nats.request(
                    subject=message["subject"],
                    payload=message.get("bytes_data", None),
                    timeout=message["timeout"],
                )
                handle = partial(self.handle_message, is_reply=True)

                await handle(response)
            except ErrTimeout:
                pass

        else:
            raise ValueError("Invalid message sent to the protocol layer.")

    async def handle_message(self, message: typing.Any, is_reply: bool = False) -> None:
        """
        Message from the NATS subscription.
        """

        # Send the message to the application queue and let the
        # ASGI application handle the message.
        if self.application_queue:
            await self.application_queue.put(
                {
                    "type": "nats.receive",
                    "subject": message.subject,
                    "reply": message.reply,
                    "is_reply": is_reply,
                    "bytes_data": message.data,
                }
            )

    async def handle_connect(self) -> None:
        if self.application_queue:
            await self.application_queue.put(
                {"type": "nats.connect", "subject": self.subject,}
            )

    async def handle_disconnect(self) -> None:
        if self.application_queue:
            await self.application_queue.put(
                {"type": "nats.disconnect", "subject": self.subject,}
            )


class NATSProtocolFactory:

    protocol_class = NATSProtocol

    def __init__(self, *, client: "Client") -> None:
        self.client = client

    def create_protocol(self, subject: str) -> NATSProtocol:
        logger.info("Initializing protocol for {}".format(subject))

        protocol = self.protocol_class(client=self.client, subject=subject)

        return protocol
