import datetime
import typing
import logging

logger = logging.getLogger(__name__)


class AccessLogGenerator:
    """
    Object that implements the channels-nats "action logger" internal interface in
    order to provide an access log in something resembling NCSA format.
    """

    def __init__(self, stream: typing.IO) -> None:
        self.stream = stream

    def __call__(self, protocol: str, action: str, details: typing.Dict) -> None:
        """
        Called when an action happens; use it to generate log entries.
        """
        # NATS requests
        if protocol == "nats" and action == "connecting":
            self.write_entry(
                date=datetime.datetime.now(), request="NATSCONNECTING",
            )
        elif protocol == "nats" and action == "connected":
            self.write_entry(
                date=datetime.datetime.now(), request="NATSCONNECT",
            )

        elif protocol == "nats" and action == "disconnected":
            self.write_entry(
                date=datetime.datetime.now(), request="NATSDISCONNECT",
            )
        elif protocol == "nats" and action == "reconnected":
            self.write_entry(
                date=datetime.datetime.now(), request="NATSRECONNECT",
            )
        elif protocol == "nats" and action == "error":
            self.write_entry(
                date=datetime.datetime.now(), request="NATSERROR %(error)s" % details,
            )
        elif protocol == "nats" and action == "closed":
            self.write_entry(
                date=datetime.datetime.now(), request="NATSCLOSED",
            )

        else:
            logger.warning(
                "AccessLogGenerator received unknown protocol %s with action %s",
                protocol,
                action,
            )

    def write_entry(self, date: datetime.datetime, request: str) -> None:
        self.stream.write("%s %s\n" % (date.strftime("%d/%b/%Y:%H:%M:%S"), request,))
