from typing import Any, Literal, LiteralString

import psycopg
from psycopg.sql import SQL, Identifier, Placeholder

TableName = Literal["events", "products", "orders", "tickets"]


async def ensure_row(
    aconn: psycopg.AsyncConnection,
    table_name: TableName,
    returning: tuple[LiteralString, ...] = (),
    unique_fields: tuple[LiteralString, ...] = ("slug",),
    **kwargs,
) -> tuple:
    """
    Ensure that a row exists in a table.
    """
    insert_sql = SQL("""
        insert into {table_name} ({fields})
        values ({value_placeholders})
        on conflict ({unique_fields}) do nothing
        returning ({returning})
    """).format(
        table_name=Identifier(table_name),
        fields=SQL(", ").join(Identifier(key) for key in kwargs),
        value_placeholders=SQL(", ").join(Placeholder() for _ in kwargs),
        returning=SQL(", ").join(Identifier(col) for col in returning),
        unique_fields=SQL(", ").join(Identifier(col) for col in unique_fields),
    )

    async with aconn.cursor() as acur:
        await acur.execute(insert_sql, tuple(kwargs.values()))

        row = await acur.fetchone()

        if row is not None:
            # inserted
            # NOTE: unintuitive behaviour of psycopg/postgres:
            # if there is only one field in RETURNING, each row has one scalar field
            # if there are more than one fields, each row has one tuple field containing all RETURNING fields
            return row[0] if len(returning) > 1 else row

        # already exists
        # RETURNING only returns created row, not the existing one
        # so need to do an explicit SELECT
        row = await get_row(
            aconn, table_name, *{key: kwargs[key] for key in unique_fields}
        )

        if row is None:
            raise AssertionError(
                "INSERT ON CONFLICT (…) DO NOTHING RETURNING (…) returned nothing, "
                "implying that the row already existed; "
                "but SELECT returned nothing too"
            )

        return row


class MultipleRowsReturned(RuntimeError):
    pass


async def get_row(
    aconn: psycopg.AsyncConnection,
    table_name: TableName,
    **kwargs,
) -> tuple[Any, ...]:
    """
    Get a single row from a table.
    """
    select_sql = SQL("""
        select *
        from {table_name}
        where {where}
        limit 2
    """).format(
        table_name=Identifier(table_name),
        where=SQL(" and ").join(
            SQL("{field_name} = {field_value}").format(
                field_name=Identifier(key),
                field_value=Placeholder(),
            )
            for key in kwargs
        ),
    )

    async with aconn.cursor() as acur:
        await acur.execute(select_sql, tuple(kwargs.values()))

        if acur.rowcount > 1:
            raise MultipleRowsReturned((kwargs, acur.rowcount))

        row = await acur.fetchone()

        if row is None:
            raise KeyError(kwargs)

        return row
