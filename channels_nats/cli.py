import logging
import functools
import typing
import argparse
import sys
import asyncio

from asgiref.compatibility import is_double_callable
from .utils import import_by_path
from .client import Client
from .access import AccessLogGenerator

logger = logging.getLogger(__name__)


class ASGI3Middleware:
    def __init__(self, app):  # type: ignore
        self.app = app

    def __call__(self, scope):  # type: ignore
        scope.setdefault("asgi", {})
        scope["asgi"]["version"] = "3.0"
        return functools.partial(self.asgi, scope=scope)

    async def asgi(self, receive, send, scope):  # type: ignore
        await self.app(scope, receive, send)


class CommandLineInterface:

    description = "Django Channels NATS interface server"

    client_class = Client

    def __init__(self):  # type: ignore
        self.parser = argparse.ArgumentParser(description=self.description)
        self.parser.add_argument(
            "application",
            help="The application to dispatch to as path.to.module:instance.path",
        )
        self.parser.add_argument(
            "-n",
            "--nats",
            nargs="+",
            dest="nats",
            help="The nats server to connect to",
            default=["nats://127.0.0.1:4222"],
        )
        self.parser.add_argument(
            "-s",
            "--subject",
            nargs="+",
            dest="subject",
            help="The nats subjects to subscribe to",
            default=[">"],
        )
        self.parser.add_argument(
            "-q",
            "--queue",
            dest="queue",
            help="The queue name used to make sure only one worker consume each message",
            default="channels-nats",
        )
        self.parser.add_argument(
            "-v",
            "--verbosity",
            type=int,
            help="How verbose to make the output",
            default=1,
        )
        self.parser.add_argument(
            "--asgi-protocol",
            dest="asgi_protocol",
            help="The version of the ASGI protocol to use",
            default="auto",
            choices=["asgi2", "asgi3", "auto"],
        )
        self.parser.add_argument(
            "--client-name",
            dest="client_name",
            help="The client name used to identify the worker",
            default="channels-nats",
        )

    @classmethod
    def entrypoint(cls):  # type: ignore
        """
        Main entrypoint for external starts.
        """
        cls().run(sys.argv[1:])  # type: ignore

    def run(self, args):  # type: ignore
        """
        Pass in raw argument list and it will decode them
        and run the server.
        """
        # Decode args
        args = self.parser.parse_args(args)

        # Set up logging
        logging.basicConfig(
            level={
                0: logging.WARN,
                1: logging.INFO,
                2: logging.DEBUG,
                3: logging.DEBUG,  # Also turns on asyncio debug
            }[args.verbosity],
            format="%(asctime)-15s %(levelname)-8s %(message)s",
        )

        # Import application
        sys.path.insert(0, ".")
        application = import_by_path(args.application)

        asgi_protocol = args.asgi_protocol
        if asgi_protocol == "auto":
            asgi_protocol = "asgi2" if is_double_callable(application) else "asgi3"

        if asgi_protocol == "asgi3":
            application = ASGI3Middleware(application)  # type: ignore

        access_log_stream = sys.stdout
        loop = asyncio.get_event_loop()

        # Start the server
        logger.info("Starting client")

        client = self.client_class(
            application=application,
            subjects=args.subject,
            verbosity=args.verbosity,
            client_name=args.client_name,
            action_logger=AccessLogGenerator(access_log_stream),
            loop=loop,
        )

        if not all([args.application, args.nats, args.client_name]):
            raise ValueError(
                "application, --nats, and --client-name are required arguments. "
                "Please add them via the command line."
            )

        loop.run_until_complete(
            client.connect(servers=args.nats, client_name=args.client_name)
        )

        try:
            client.run()
        except KeyboardInterrupt:
            # Drain nats connection
            loop.run_until_complete(client.disconnect())

            # Wait on pending tasks
            tasks = asyncio.gather(
                *asyncio.Task.all_tasks(loop=loop), loop=loop, return_exceptions=True
            )
            tasks.add_done_callback(lambda t: loop.stop())
            tasks.cancel()

            while not tasks.done() and not loop.is_closed():
                loop.run_forever()
        finally:
            loop.close()
            sys.exit(0)
