import typing

import asyncio
import logging
import time
import sys
from nats import NATS

from .protocol import NATSProtocolFactory

if typing.TYPE_CHECKING:
    from .access import AccessLogGenerator

logger = logging.getLogger(__name__)


class Client:
    """
    The client class is designed to handle multiple queue subscriptions and
    send/receive messages between the asgi app and the NATS pubsub client.
    """

    def __init__(
        self,
        *,
        application: typing.Any,
        subjects: typing.List[str],
        verbosity: int,
        client_name: str,
        action_logger: "AccessLogGenerator",
        loop: asyncio.AbstractEventLoop
    ):
        self.application = application
        self.subjects = subjects
        self.verbosity = verbosity
        self.client_name = client_name
        self.action_logger = action_logger

        self.loop = loop

        self.nats: typing.Any = NATS()
        self.application_close_timeout = 2
        self.is_connected: asyncio.Event = asyncio.Event()

    def run(self) -> None:
        # A dict of { protocol: {"application_instance":, "connected":, "disconnected":}}
        self.connections: typing.Dict[typing.Any, typing.Dict[str, typing.Any]] = {}

        # Make the factory
        self.factory = NATSProtocolFactory(client=self)

        # Setup protocols
        task = self.loop.create_task(self.setup())
        task.add_done_callback(self.setup_complete)

        # Start the application checker
        self.loop.call_later(1, self.application_checker)

        self.loop.run_forever()

    async def connect(self, servers: typing.List[str], client_name: str) -> None:
        logger.info("Connecting to NATS Server {}".format(servers))
        self.log_action("nats", "connecting", {})
        await self.nats.connect(
            loop=self.loop,
            servers=servers,
            name=client_name,
            disconnected_cb=self.disconnected_cb,
            reconnected_cb=self.reconnected_cb,
            error_cb=self.error_cb,
            closed_cb=self.closed_cb,
        )

        if self.nats.is_connected:
            logging.info(
                "Connected to NATS Server {}".format(self.nats.connected_url.geturl())
            )
            self.log_action("nats", "connected", {})
            self.on_connected()
        else:
            raise Exception("Could not connect to NATS") from self.nats.last_error

    async def disconnect(self) -> None:
        logger.info("Stopping all applications")
        await self.kill_all_applications()

        logger.info("Draining NATS connection")
        await self.nats.flush()
        await self.nats.drain()

    async def setup(self) -> None:
        """
        Create the required protocols based on the given subjects.
        """

        for subject in self.subjects:
            self.factory.create_protocol(subject=subject)

    def setup_complete(self, future: asyncio.Future) -> None:
        try:
            future.result()
        except Exception:
            logger.exception("Failed to setup subject subscriptions")

        # Start the subscription checker
        self.loop.call_later(1, self.subscription_checker)

    #
    # NATS callbacks
    #

    async def disconnected_cb(self) -> None:
        self.log_action("nats", "disconnected", {})
        self.on_disconnected()

    async def reconnected_cb(self) -> None:
        self.log_action("nats", "reconnected", {})
        self.on_connected()

    async def error_cb(self, exc: Exception) -> None:
        self.log_action("nats", "error", {"error": str(exc)})

    async def closed_cb(self) -> None:
        self.log_action("nats", "closed", {})
        self.on_disconnected()

        sys.exit(1)

    #
    # Protocol handling
    #

    def protocol_connected(self, protocol: typing.Any) -> None:
        """
        Adds a protocol as a current connection.
        """

        if protocol in self.connections:
            raise RuntimeError("Protocol %r was added to main list twice!" % protocol)

        self.connections[protocol] = {"connected": time.time()}

    def protocol_disconnected(self, protocol: typing.Any) -> None:
        if "disconnected" not in self.connections[protocol]:
            self.connections[protocol]["disconnected"] = time.time()

    def create_application(
        self, protocol: typing.Any, scope: typing.Dict
    ) -> typing.Optional[asyncio.Queue]:
        """
        Creates a new application instance that fronts a Protocol instance
        for one of our supported protocols. Pass it the protocol,
        and it will work out the type, supply appropriate callables, and
        return you the application's input queue
        """

        # Make sure the protocol has not had another application made for it
        assert "application_instance" not in self.connections[protocol]

        # Make an instance of the application
        input_queue = asyncio.Queue()  # type: ignore
        application_instance = self.application(scope=scope)

        # Run it, and stash the future for later checking
        if protocol not in self.connections:
            return None

        self.connections[protocol]["application_instance"] = asyncio.ensure_future(
            application_instance(
                receive=input_queue.get,
                send=lambda message: self.handle_reply(protocol, message),
            ),
            loop=self.loop,
        )

        return input_queue

    async def handle_reply(self, protocol: typing.Any, message: typing.Any) -> None:
        """
        Coroutine that jumps the reply message from asyncio to NATS
        """

        # Don't do anything if the connection is closed or does not exist
        if protocol not in self.connections or self.connections[protocol].get(
            "disconnected", None
        ):
            return

        # Let the protocol handle it
        await protocol.handle_reply(message)

    #
    # Utility
    #

    def application_checker(self) -> None:
        """
        Goes through the set of current application Futures and cleans up
        any that are done/prints exceptions for any that errored.
        """

        for protocol, details in list(self.connections.items()):

            disconnected = details.get("disconnected", None)
            application_instance = details.get("application_instance", None)

            # First, see if the protocol disconnected and the app has taken
            # too long to close up
            if (
                disconnected
                and time.time() - disconnected > self.application_close_timeout
            ):
                if application_instance and not application_instance.done():
                    logger.warning(
                        "Application instance %r for connection %s took too long to shut down and was killed.",
                        application_instance,
                        repr(protocol),
                    )
                    application_instance.cancel()

            # Then see if the app is done and we should reap it
            if application_instance and application_instance.done():
                try:
                    exception = application_instance.exception()

                except asyncio.CancelledError:
                    # Future cancellation. We can ignore this.
                    pass

                else:
                    if exception:
                        logger.error(
                            "Exception inside application: %s",
                            exception,
                            exc_info=exception,
                        )
                        if not disconnected:
                            protocol.handle_exception(exception)

                del self.connections[protocol]["application_instance"]
                application_instance = None

            # Check to see if protocol is closed and app is closed so we can remove it
            if not application_instance and disconnected:
                del self.connections[protocol]

        self.loop.call_later(1, self.application_checker)

    async def kill_all_applications(self) -> None:
        """
        Kills all application coroutines before client exit.
        """

        # Send cancel to all coroutines
        wait_for = []

        for protocol, details in self.connections.items():
            # Application instance
            application_instance = details["application_instance"]
            if not application_instance.done():
                application_instance.cancel()
                wait_for.append(application_instance)

        logger.info(
            "Killed %i pending application instances", len(wait_for)
        )

        await asyncio.gather(*wait_for, return_exceptions=True)

    def subscription_checker(self) -> None:
        """
        Make sure each subject have a configured application.
        """

        subscribed_subjects = {protocol.subject for protocol in self.connections.keys()}
        missing_subjects = set(self.subjects) - subscribed_subjects

        # Initialize missing subject subscriptions
        for subject in missing_subjects:
            self.factory.create_protocol(subject=subject)

        self.loop.call_later(1, self.subscription_checker)

    def on_connected(self) -> None:
        self.is_connected.set()

    def on_disconnected(self) -> None:
        self.is_connected.clear()

    def log_action(self, protocol: str, action: str, details: typing.Dict) -> None:
        """
        Dispatches to any registered action logger, if there is one.
        """

        self.action_logger(protocol, action, details)
