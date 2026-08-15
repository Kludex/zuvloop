import asyncio

import grpc


async def ping(request: bytes, context: grpc.aio.ServicerContext[bytes, bytes]) -> bytes:
    del context
    assert request == b"ping"
    return b"pong"


async def main() -> None:
    assert type(asyncio.get_running_loop()).__module__.startswith("zuvloop")

    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "zuvloop.Compatibility",
                {
                    "Ping": grpc.unary_unary_rpc_method_handler(
                        ping,
                        request_deserializer=bytes,
                        response_serializer=bytes,
                    )
                },
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            call = channel.unary_unary(
                "/zuvloop.Compatibility/Ping",
                request_serializer=bytes,
                response_deserializer=bytes,
            )
            assert await call(b"ping") == b"pong"
    finally:
        await server.stop(grace=None)


asyncio.run(main())
