import psycopg
import pytest
from emticketen.models import create_tables
from psycopg_pool import AsyncConnectionPool


async def _configure(aconn: psycopg.AsyncConnection):
    async with aconn.transaction():
        await aconn.execute("set search_path to emticketen_test")


@pytest.fixture
async def apool():
    # `brew install postgresql` sets max_connections = 80
    async with AsyncConnectionPool(configure=_configure, max_size=70) as apool:
        async with apool.connection() as aconn:
            await aconn.execute("drop schema if exists emticketen_test cascade")
            await aconn.execute("create schema emticketen_test")
            await create_tables(aconn)

        yield apool


@pytest.fixture
async def aconn(apool: AsyncConnectionPool):
    async with apool.connection() as aconn:
        yield aconn
