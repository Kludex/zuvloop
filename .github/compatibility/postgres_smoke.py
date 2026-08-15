import asyncio

import asyncpg
import psycopg


async def main() -> None:
    assert type(asyncio.get_running_loop()).__module__.startswith("zuvloop")

    asyncpg_connection = await asyncpg.connect(
        host="127.0.0.1",
        user="postgres",
        password="postgres",
        database="postgres",
    )
    try:
        assert await asyncpg_connection.fetchval("SELECT 42") == 42
    finally:
        await asyncpg_connection.close()

    async with await psycopg.AsyncConnection.connect(
        "host=127.0.0.1 user=postgres password=postgres dbname=postgres"
    ) as psycopg_connection:
        async with psycopg_connection.cursor() as cursor:
            await cursor.execute("SELECT 42")
            assert await cursor.fetchone() == (42,)


asyncio.run(main())
