import pytest
from emticketen.models import Event, NotEnoughTickets, Order, Product, Ticket
from psycopg_pool import AsyncConnectionPool


async def test_concurrent(apool: AsyncConnectionPool):
    async with apool.connection() as aconn:
        event = await Event.ensure(aconn, "test-event")
        product = await Product.ensure(aconn, event, "test-product", 10)

    async with apool.connection() as aconn1, apool.connection() as aconn2:
        await aconn1.execute("begin")
        await aconn2.execute("begin")

        order1 = await Order.create(aconn1, event)
        order2 = await Order.create(aconn2, event)

        await Ticket.reserve(aconn1, order1, product, 6)

        # magic! aconn1 still holding transaction open
        with pytest.raises(NotEnoughTickets):
            await Ticket.reserve(aconn2, order2, product, 7)

        await aconn1.execute("commit")
        await aconn2.execute("rollback")
