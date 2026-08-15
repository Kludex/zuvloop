import asyncio

from websockets.asyncio.client import connect
from websockets.asyncio.server import ServerConnection, serve


async def echo(websocket: ServerConnection) -> None:
    async for message in websocket:
        await websocket.send(message)


async def main() -> None:
    assert type(asyncio.get_running_loop()).__module__.startswith("zuvloop")

    async with serve(echo, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with connect(f"ws://127.0.0.1:{port}") as websocket:
            await websocket.send("hello")
            assert await websocket.recv() == "hello"
            pong = await websocket.ping(b"zuvloop")
            await pong


asyncio.run(main())
