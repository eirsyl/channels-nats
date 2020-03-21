import typing


class Application:
    def __init__(self, scope: typing.Dict) -> None:
        self.scope = scope
        print("SCOPE", scope)

    async def __call__(
        self,
        receive: typing.Callable[..., typing.Awaitable[typing.Dict]],
        send: typing.Callable[..., typing.Awaitable[None]],
    ) -> None:
        i = 1

        while True:
            msg = await receive()

            if msg["type"] in ["nats.connect", "nats.disconnect"]:
                print("Conn EVENT: ", msg)
                continue

            print("MESSAGE: ", msg, "counter", i)

            # await send(
            #     {
            #         "type": "nats.request",
            #         "subject": "whale.reply",
            #         "bytes_data": msg["bytes_data"],
            #         "timeout": 3,
            #     }
            # )

            if i % 5 == 0:
                # Raise exception to test exception handling
                raise ValueError("Catch this :)")

            i += 1


application = Application
