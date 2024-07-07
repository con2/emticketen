import psycopg
import pytest
from emticketen.models import Event, NotEnoughTickets, Order, Product, Ticket


async def test_relaxed(aconn: psycopg.AsyncConnection):
    """
    Simple, relaxed test case without contention.
    """
    event = await Event.ensure(aconn, "test-event")
    product = await Product.ensure(aconn, event, "test-product", 10)
    order = await Order.create(aconn, event)

    await Ticket.reserve(aconn, order, product, 3)
    await Ticket.reserve(aconn, order, product, 3)
    await Ticket.reserve(aconn, order, product, 3)

    with pytest.raises(NotEnoughTickets):
        # NOTE: psycopg3's automatic transaction management makes this a SAVEPOINT
        async with aconn.transaction():
            await Ticket.reserve(aconn, order, product, 3)

    num_free_tickets = await Ticket.count_free(aconn, product)
    assert num_free_tickets == 1

    availability = await Product.get_products_with_availability(aconn, event)
    product, available = availability[0]
    assert product.slug == "test-product"
    assert available

    await Ticket.reserve(aconn, order, product, 1)

    availability = await Product.get_products_with_availability(aconn, event)
    product, available = availability[0]
    assert product.slug == "test-product"
    assert not available
