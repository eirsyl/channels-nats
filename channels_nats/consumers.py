from channels.consumer import AsyncConsumer, SyncConsumer


class AsyncNATSConsumer(AsyncConsumer):
    """
    Base NATS consumer; Implements basic hooks for interfacing with the NATS Interface Server
    """

    async def nats_receive(self, message):
        """
        Parses incoming messages and routes them to the appropriate handler, depending on the
        incoming action type
        """

        await self.receive(
            subject=message["subject"],
            bytes_data=message["bytes"],
            reply=message.get("reply"),
        )

    async def send(self, subject, bytes_data):
        """
        Sends a command to the NATS Server.  Message should be of the format:
            {
                'type': 'nats.send',
                'subject': '<SUBJECT>',
                'bytes': '<BYTES_DATA>',
            }
        """

        await super().send(
            {"type": "nats.send", "subject": subject, "bytes": bytes_data}
        )

    #
    # Methods to be implemented by the consumer classes
    #

    async def receive(self, subject, bytes_data, reply=None):
        pass


class NATSConsumer(SyncConsumer):
    def nats_receive(self, message):
        """
        Parses incoming messages and routes them to the appropriate handler, depending on the
        incoming action type
        """

        self.receive(
            subject=message["subject"],
            bytes_data=message["bytes"],
            reply=message.get("reply"),
        )

    def send(self, subject, bytes_data):
        """
        Sends a command to the NATS Server.  Message should be of the format:
            {
                'type': 'nats.send',
                'subject': '<SUBJECT>',
                'bytes': '<BYTES_DATA>',
            }
        """

        super().send({"type": "nats.send", "subject": subject, "bytes": bytes_data})

    #
    # Methods to be implemented by the consumer classes
    #

    def receive(self, subject, bytes_data, reply=None):
        pass


class SubjectRouter:
    """
    Takes a mapping of subject names to other Application instances,
    and dispatches to the right one based on subject (or raises an error)
    """

    def __init__(self, application_mapping):
        self.application_mapping = application_mapping

    def __call__(self, scope):
        if scope["subject"] in self.application_mapping:
            return self.application_mapping[scope["subject"]](scope)
        else:
            raise ValueError(
                "No application configured for scope subject %r" % scope["subject"]
            )
