import asyncio

from redis.asyncio import Redis


async def main() -> None:
    assert type(asyncio.get_running_loop()).__module__.startswith("zuvloop")

    client = Redis(host="127.0.0.1", decode_responses=True)
    try:
        assert await client.ping()
        await client.set("zuvloop:compatibility", "ok")
        assert await client.get("zuvloop:compatibility") == "ok"
    finally:
        await client.aclose()


asyncio.run(main())
