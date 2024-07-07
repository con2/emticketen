from typing import Any, Literal, LiteralString, Self

import psycopg
from psycopg.sql import SQL, Identifier, Placeholder
from pydantic import BaseModel

from .utils import ensure_row, get_row


class Event(BaseModel):
    """
    create table events (
        id int primary key generated always as identity,
        slug text not null,
        unique (slug)
    );
    """

    id: int
    slug: str

    @classmethod
    async def ensure(cls, aconn: psycopg.AsyncConnection, slug: str) -> Self:
        id, slug = await ensure_row(aconn, "events", ("id", "slug"), slug=slug)
        return cls(id=id, slug=slug)

    @classmethod
    async def get(cls, aconn: psycopg.AsyncConnection, slug: str) -> Self:
        id, slug = await get_row(aconn, "events", slug=slug)
        return cls(id=id, slug=slug)


class Order(BaseModel):
    """
    create table orders (
        id int primary key generated always as identity,
        event_id int not null,
        foreign key (event_id) references events (id)
    );
    """

    id: int
    event_id: int

    @classmethod
    async def create(cls, aconn: psycopg.AsyncConnection, event: Event) -> Self:
        async with aconn.cursor() as acur:
            await acur.execute(
                "insert into orders (event_id) values (%s) returning id",
                (event.id,),
            )
            row = await acur.fetchone()
            if row is None:
                raise AssertionError("INSERT RETURNING returned nothing")
            (id,) = row
            return cls(id=id, event_id=event.id)


class Product(BaseModel):
    """
    create table products (
        id int primary key generated always as identity,
        event_id int not null,
        slug text not null,
        quota int not null,
        unique (event_id, slug),
        foreign key (event_id) references events (id)
    );
    """

    id: int
    event_id: int
    slug: str
    quota: int

    @classmethod
    async def ensure(
        cls, aconn: psycopg.AsyncConnection, event: Event, slug: str, quota: int
    ) -> Self:
        id, event_id, slug, quota = await ensure_row(
            aconn,
            "products",
            returning=("id", "event_id", "slug", "quota"),
            unique_fields=("event_id", "slug"),
            event_id=event.id,
            slug=slug,
            quota=quota,
        )

        product = cls(id=id, event_id=event_id, slug=slug, quota=quota)

        await product.ensure_tickets(aconn)

        return product

    async def ensure_tickets(self, aconn: psycopg.AsyncConnection):
        """
        Ensure that the product has the correct number of tickets.
        """

        async with aconn.cursor() as acur:
            await acur.execute(
                "select count(*) from tickets where product_id = %s", (self.id,)
            )
            count = row[0] if (row := await acur.fetchone()) else 0

            if count == self.quota:
                return
            elif count > self.quota:
                raise NotImplementedError("deleting tickets is not implemented")
            else:
                await acur.execute(
                    "insert into tickets (product_id) select %s from generate_series(1, %s) as _",
                    (self.id, self.quota - count),
                )

    @classmethod
    async def get(cls, aconn: psycopg.AsyncConnection, event: Event, slug: str) -> Self:
        id, event_id, slug, quota = await get_row(
            aconn,
            "products",
            event_id=event.id,
            slug=slug,
        )
        return cls(id=id, event_id=event_id, slug=slug, quota=quota)

    @classmethod
    async def get_products_with_availability(
        cls,
        aconn: psycopg.AsyncConnection,
        event: Event,
    ) -> list[tuple[Self, bool]]:
        async with aconn.cursor() as acur:
            await acur.execute(
                # TODO can the correlated subquery be avoided?
                # need to limit 1 per product, not altogether
                # (then again, products is low cardinality, so it's not a big deal)
                """
                select
                    id,
                    slug,
                    quota,
                    (
                        select id
                        from tickets
                        where product_id = products.id and order_id is null
                        limit 1
                        for update
                        skip locked
                    ) is not null as available
                from products
                where event_id = %s
                """,
                (event.id,),
            )

            return [
                (
                    cls(
                        id=id,
                        event_id=event.id,
                        slug=slug,
                        quota=quota,
                    ),
                    available,
                )
                for (id, slug, quota, available) in await acur.fetchall()
            ]


class NotEnoughTickets(RuntimeError):
    pass


class Ticket(BaseModel):
    """
    create table tickets (
        id int primary key generated always as identity,
        product_id int not null,
        order_id int,
        foreign key (product_id) references products (id),
        foreign key (order_id) references orders (id)
    );

    comment on table tickets is 'A "ticket" is an instance of a product. All salable instances of a product must be realized as tickets.';
    """

    id: int
    product_id: int
    order_id: int

    @classmethod
    async def reserve(
        cls,
        aconn: psycopg.AsyncConnection,
        order: Order,
        product: Product,
        quantity: int,
    ):
        async with aconn.cursor() as acur:
            await acur.execute(
                """
                with reserved as (
                    select id
                    from tickets
                    where product_id = %s and order_id is null
                    limit %s
                    for update
                    skip locked
                )
                update tickets
                set order_id = %s
                from reserved
                where tickets.id = reserved.id
                returning tickets.id
                """,
                (product.id, quantity, order.id),
            )

            rows = await acur.fetchall()

            if len(rows) < quantity:
                raise NotEnoughTickets()

            return [
                cls(
                    id=id,
                    product_id=product.id,
                    order_id=order.id,
                )
                for (id,) in rows
            ]

    @classmethod
    async def count_free(
        cls,
        aconn: psycopg.AsyncConnection,
        product: Product,
    ) -> int:
        async with aconn.cursor() as acur:
            await acur.execute(
                "select count(*) from tickets where product_id = %s and order_id is null",
                (product.id,),
            )
            row = await acur.fetchone()
            return row[0] if row else 0


async def create_tables(aconn: psycopg.AsyncConnection):
    """
    Create tables, extracting SQL from dataclass docstrings.
    """
    async with aconn.cursor() as acur:
        for cls in (Event, Product, Order, Ticket):
            # psycopg typings protect us from SQL injection
            # however, we know that cls.__doc__ is a literal string written by us
            await acur.execute(cls.__doc__)  # type: ignore
        await aconn.commit()
