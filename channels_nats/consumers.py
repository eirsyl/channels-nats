import typing
from channels.consumer import AsyncConsumer, SyncConsumer


class AsyncNATSConsumer(AsyncConsumer):  # type: ignore
    """
    Base NATS consumer; Implements basic hooks for interfacing with the NATS Interface Server
    """

    async def nats_receive(self, message: typing.Dict) -> None:
        """
        Parses incoming messages and routes them to the appropriate handler, depending on the
        incoming action type
        """

        await self.receive(
            subject=message["subject"],
            bytes_data=message["bytes_data"],
            reply=message.get("reply"),
            is_reply=message.get("is_reply", False),
        )

    async def send(self, subject: str, bytes_data: bytes) -> None:
        """
        Sends a message to the NATS Server. Message should be of the format:
            {
                'type': 'nats.send',
                'subject': '<SUBJECT>',
                'bytes_data': '<BYTES_DATA>',
            }
        """

        await super().send(
            {"type": "nats.send", "subject": subject, "bytes_data": bytes_data}
        )

    async def request(self, subject: str, bytes_data: bytes, timeout: int = 1) -> None:
        """
        Sends a request message to the NATS Server.  Message should be of the format:
            {
                'type': 'nats.request',
                'subject': '<SUBJECT>',
                'bytes_data': '<BYTES_DATA>',
                'timeout': '<TIMEOUT>',
            }
        """

        await super().send(
            {
                "type": "nats.request",
                "subject": subject,
                "bytes_data": bytes_data,
                "timeout": timeout,
            }
        )

    #
    # Methods to be implemented by the consumer classes
    #

    async def nats_connect(self, message: typing.Dict) -> None:
        pass

    async def nats_disconnect(self, message: typing.Dict) -> None:
        pass

    async def receive(
        self,
        subject: str,
        bytes_data: bytes,
        reply: typing.Optional[str],
        is_reply: bool,
    ) -> None:
        pass


class NATSConsumer(SyncConsumer):  # type: ignore
    def nats_receive(self, message: typing.Dict) -> None:
        """
        Parses incoming messages and routes them to the appropriate handler, depending on the
        incoming action type
        """

        self.receive(
            subject=message["subject"],
            bytes_data=message["bytes_data"],
            reply=message.get("reply"),
            is_reply=message.get("is_reply", False),
        )

    def send(self, subject: str, bytes_data: bytes) -> None:
        """
        Sends a message to the NATS Server.  Message should be of the format:
            {
                'type': 'nats.send',
                'subject': '<SUBJECT>',
                'bytes_data': '<BYTES_DATA>',
            }
        """

        super().send(
            {"type": "nats.send", "subject": subject, "bytes_data": bytes_data}
        )

    def request(self, subject: str, bytes_data: bytes, timeout: int = 1) -> None:
        """
        Sends a request message to the NATS Server.  Message should be of the format:
            {
                'type': 'nats.request',
                'subject': '<SUBJECT>',
                'bytes_data': '<BYTES_DATA>',
                'timeout': '<TIMEOUT>',
            }
        """

        super().send(
            {
                "type": "nats.request",
                "subject": subject,
                "bytes_data": bytes_data,
                "timeout": timeout,
            }
        )

    #
    # Methods to be implemented by the consumer classes
    #

    def nats_connect(self, message: typing.Dict) -> None:
        pass

    def nats_disconnect(self, message: typing.Dict) -> None:
        pass

    def receive(
        self,
        subject: str,
        bytes_data: bytes,
        reply: typing.Optional[str],
        is_reply: bool,
    ) -> None:
        pass


class SubjectRouter:
    """
    Takes a mapping of subject names to other Application instances,
    and dispatches to the right one based on subject (or raises an error)
    """

    def __init__(self, application_mapping: typing.Dict[str, typing.Any]) -> None:
        self.application_mapping = application_mapping

    def __call__(self, scope: typing.Dict) -> typing.Any:
        if scope["subject"] in self.application_mapping:
            return self.application_mapping[scope["subject"]](scope)
        else:
            raise ValueError(
                "No application configured for scope subject %r" % scope["subject"]
            )
