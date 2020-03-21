# type: ignore
import asyncio
import traceback
import logging
import typing
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass()
class ApplicationInstance:
    """
    Dataclass used to store the application instance for each subject subscription.
    """

    queue: asyncio.Queue
    application: typing.Any
    future: asyncio.Future
    subscription: typing.Any = None


class BaseServer:
    """
    Base class for common interface server functions, including
    creating an application, sending to the consumer, and application_checking/
    error handling
    """

    async def _send_application_msg(self, application_instance, msg):
        """
        Sends a msg (serializable dict) to the appropriate Django channel
        """

        return await application_instance.queue.put(msg)

    def noop_from_consumer(self, msg):
        """
        Empty default for receiving from consumer
        """

    def create_application(self, scope={}, from_consumer=noop_from_consumer):
        """
        Handles creating the ASGI application and instatiating the
        send Queue
        """
        application_instance = self.application(scope=scope)
        application_queue = asyncio.Queue(loop=self.loop)
        future = asyncio.ensure_future(
            application_instance(receive=application_queue.get, send=from_consumer),
            loop=self.loop,
        )

        return ApplicationInstance(
            queue=application_queue, application=application_instance, future=future
        )

    def futures_checker(self):
        """
        Looks for exeptions raised in the application
        """
        for application_instance in self.subscriptions.values():

            try:
                exception = application_instance.future.exception()
            except asyncio.CancelledError:
                # Future cancellation. We can ignore this.
                pass
            else:
                if exception:
                    exception_output = "{}\n{}{}".format(
                        exception,
                        "".join(traceback.format_tb(exception.__traceback__,)),
                        "  {}".format(exception),
                    )
                    logger.error(
                        "Exception inside application: %s", exception_output,
                    )

        self.loop.call_later(1, self.futures_checker)
