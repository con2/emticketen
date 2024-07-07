from contextlib import asynccontextmanager

import psycopg
from psycopg_pool import AsyncConnectionPool


async def _configure(aconn: psycopg.AsyncConnection):
    # NOTE: if not wrapped in a transaction, this will trigger psycopg's implicit transaction
    async with aconn.transaction():
        await aconn.execute("set search_path to emticketen")


@asynccontextmanager
async def async_connection():
    async with await psycopg.AsyncConnection.connect() as aconn:
        await _configure(aconn)
        yield aconn


@asynccontextmanager
async def async_pool():
    async with AsyncConnectionPool(configure=_configure, open=False) as apool:
        yield apool
