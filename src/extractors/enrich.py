import duckdb
import pandas as pd
import polars as pl


def polars_join(
    left, right, left_on, right_on, left_fields: list, right_fields: list, how="left"
):
    def select_and_rename(data, fields):
        df = pl.from_dicts(data, infer_schema_length=None)
        if "*" in fields:
            select_cols = df.columns
        else:
            select_cols = [f[0] if isinstance(f, tuple) else f for f in fields]

        rename_map = {
            src: dst for f in fields if isinstance(f, tuple) for src, dst in [f]
        }

        df = df.select(select_cols)
        if rename_map:
            df = df.rename(rename_map)

        return df

    df_left = select_and_rename(left, left_fields + [left_on])
    df_right = select_and_rename(right, right_fields + [right_on])

    df_joined = df_left.join(df_right, left_on=left_on, right_on=right_on, how=how)

    return df_joined.to_dicts()


def join(
    left, right, left_on, right_on, left_fields: list, right_fields: list, how="left"
):
    def select_and_rename(table_name, data, key_field, fields, conn):
        df = pd.DataFrame(data)

        conn.register(table_name, df)
        rename_map = {
            src: dst for f in fields if isinstance(f, tuple) for src, dst in [f]
        }
        if "*" in fields:
            fields.remove("*")
            fields.extend(
                [f for f in df.columns if f not in rename_map and f != key_field]
            )

        select_parts = [
            f"{col}" if not isinstance(col, tuple) else f"{col[0]} AS {col[1]}"
            for col in fields
        ]

        return select_parts

    conn = duckdb.connect(database=":memory:")

    left_select = select_and_rename("left_tbl", left, left_on, left_fields, conn)
    left_select.append("left_tbl." + left_on)
    right_select = select_and_rename("right_tbl", right, right_on, right_fields, conn)

    query = f"""
        SELECT {", ".join(left_select + right_select)}
        FROM left_tbl
        {how.upper()} JOIN right_tbl
        ON left_tbl.{left_on} = right_tbl.{right_on}
    """

    result = conn.execute(query).fetchdf()  # returns pandas DataFrame
    return result.to_dict(orient="records")


async def enrich_event(events, blocks, pools, tokens, binance_client):
    from src.fetchers.eth_price import EthPriceFetcher

    timestamps = [block["timestamp"] * 1000 for block in blocks]
    fetcher = EthPriceFetcher(binance_client)
    prices = await fetcher.run(timestamps=timestamps)

    prices = [
        {
            "block_number": block["number"],
            "block_timestamp": block["timestamp"],
            "eth_price": float(price["p"]),
        }
        for price, block in zip(prices, blocks)
    ]

    results = list(
        join(
            events,
            prices,
            "block_number",
            "block_number",
            ["*"],
            [
                "block_timestamp",
                "eth_price",
            ],
        )
    )

    results = list(
        join(
            results,
            pools,
            "pool_address",
            "pool_address",
            ["*"],
            [
                "token0_address",
                "token1_address",
            ],
        )
    )

    results = list(
        join(
            results,
            tokens,
            "token0_address",
            "address",
            ["*"],
            [
                ("name", "token0_name"),
                ("symbol", "token0_symbol"),
                ("decimals", "token0_decimals"),
            ],
        )
    )

    results = list(
        join(
            results,
            tokens,
            "token1_address",
            "address",
            ["*"],
            [
                ("name", "token1_name"),
                ("symbol", "token1_symbol"),
                ("decimals", "token1_decimals"),
            ],
        )
    )

    return results
