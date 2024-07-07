import asyncio
from collections import Counter
from enum import Enum, auto
from random import choice, uniform

from emticketen.models import Event, NotEnoughTickets, Order, Product, Ticket
from psycopg_pool import AsyncConnectionPool


async def _view_products_page(apool: AsyncConnectionPool):
    """
    Simulates a buyer viewing the web shop page that tells if the product is available or not.
    """
    async with apool.connection() as aconn:
        event = await Event.get(aconn, "test-event")
        result = await Product.get_products_with_availability(aconn, event)

        await asyncio.sleep(uniform(0, 0.001))

        return result


async def _buy_tickets(apool: AsyncConnectionPool, desired_amounts: dict[str, int]):
    """
    Simulates a buyer buying tickets.
    """
    async with apool.connection() as aconn:
        event = await Event.get(aconn, "test-event")
        order = await Order.create(aconn, event)

        for product_slug, quantity in desired_amounts.items():
            product = await Product.get(aconn, event, product_slug)
            await Ticket.reserve(aconn, order, product, quantity)

        await asyncio.sleep(uniform(0, 0.001))


AMOUNTS = [
    *[0] * 10,
    *[1] * 10,
    *[2] * 6,
    *[3] * 4,
    *[4] * 2,
    *[5] * 1,
]


class Result(Enum):
    SUCCESS = auto()
    SERVED_SOLD_OUT_PAGE = auto()
    NOT_ENOUGH_TICKETS = auto()
    JUST_BROWSING = auto()


async def _buyer(
    apool: AsyncConnectionPool,
    max_tardiness_seconds: int = 10,
    max_time_to_buy_seconds: int = 5,
):
    await asyncio.sleep(uniform(0, max_tardiness_seconds))

    availability = await _view_products_page(apool)
    if not any(available for _, available in availability):
        return Result.SERVED_SOLD_OUT_PAGE

    await asyncio.sleep(uniform(0, max_time_to_buy_seconds))

    # randomize desired amounts for each available product
    desired_amounts = {
        product.slug: amount
        for product, available in availability
        if available and (amount := choice(AMOUNTS))
    }

    if not desired_amounts:
        return Result.JUST_BROWSING

    try:
        await _buy_tickets(apool, desired_amounts)
    except NotEnoughTickets:
        return Result.NOT_ENOUGH_TICKETS
    else:
        return Result.SUCCESS


async def test_contested(
    apool: AsyncConnectionPool,
    num_products=5,
    num_tickets_per_product=5500,
    num_buyers=7000,
):
    async with apool.connection() as aconn:
        event = await Event.ensure(aconn, "test-event")
        for i in range(num_products):
            await Product.ensure(
                aconn, event, f"test-product-{i}", num_tickets_per_product
            )

    buyers = [_buyer(apool) for _ in range(num_buyers)]
    results = Counter(await asyncio.gather(*buyers))
    print(results)

    async with apool.connection() as aconn, aconn.cursor() as acur:
        await acur.execute("""
            select products.slug, count(*)
            from tickets
            join products on tickets.product_id = products.id
            where tickets.order_id is not null
            group by products.slug
        """)

        rows = await acur.fetchall()

        # print(rows)

        for slug, num_sold in rows:
            assert (
                num_sold == num_tickets_per_product
            ), f"{slug} has wrong amount of tickets sold"
