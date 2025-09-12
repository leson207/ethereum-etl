import polars as pl

# TODO: check field of those join later


def join(
    left, right, left_on, right_on, left_fields: list, right_fields: list, how="left"
):
    def select_and_rename(data, fields):
        df = pl.from_dicts(data, infer_schema_length=None)
        if "*" in fields:
            select_cols = df.columns
        else:
            select_cols = [f[0] if isinstance(f, tuple) else f for f in fields]

        rename_map = {src: dst for f in fields if isinstance(f, tuple) for src, dst in [f]}
        
        df = df.select(select_cols)        
        if rename_map:
            df = df.rename(rename_map)
        
        return df

    df_left = select_and_rename(left, left_fields + [left_on])
    df_right = select_and_rename(right, right_fields + [right_on])

    df_joined = df_left.join(df_right, left_on=left_on, right_on=right_on, how=how)

    return df_joined.to_dicts()

async def enrich_event(events, blocks, pools, tokens, binance_client):
    from src.extractors.enrich import join
    from src.fetchers.eth_price import EthPriceFetcher

    timestamps= [block['timestamp']*1000 for block in blocks]
    fetcher = EthPriceFetcher(binance_client)
    prices = await fetcher.run(timestamps=timestamps)

    prices = [
        {
            "block_number": block['number'],
            "block_timestamp": block['timestamp'],
            "eth_price": float(price['p'])
        }
        for price,block in zip(prices, blocks)
    ]

    results = list(
        join(
            events,
            prices,
            "block_number",
            "block_number",
            [
                "type",
                "dex",
                "pool_address",
                "amount0_in",
                "amount1_in",
                "amount0_out",
                "amount1_out",
                "transaction_hash",
                "log_index",
            ],
            [
                "block_timestamp",
                "eth_price",
            ],
        )
    )

    print('8'*100)
    results = list(
        join(
            results,
            pools,
            "pool_address",
            "pool_address",
            [
                "type",
                "dex",
                "amount0_in",
                "amount1_in",
                "amount0_out",
                "amount1_out",
                "transaction_hash",
                "log_index",
                "block_number",
                "block_timestamp",
                "eth_price"
            ],
            [
                "token0_address",
                "token1_address",
            ],
        )
    )

    print('8'*100)
    results = list(
        join(
            results,
            tokens,
            "token0_address",
            "address",
            [
                "type",
                "dex",
                "pool_address",
                "amount0_in",
                "amount1_in",
                "amount0_out",
                "amount1_out",
                "transaction_hash",
                "log_index",
                "block_number",
                "block_timestamp",
                "eth_price",
                "token1_address"
            ],
            [
                ("name", "token0_name"),
                ("symbol", "token0_symbol"),
                ("decimals", "token0_decimals")
            ],
        )
    )
    print('8'*100)
    results = list(
        join(
            results,
            tokens,
            "token1_address",
            "address",
            [
                "type",
                "dex",
                "pool_address",
                "amount0_in",
                "amount1_in",
                "amount0_out",
                "amount1_out",
                "transaction_hash",
                "log_index",
                "block_number",
                "block_timestamp",
                "eth_price",
                "token0_address",
                "token0_name",
                "token0_symbol",
                "token0_decimals"
            ],
            [
                ("name", "token1_name"),
                ("symbol", "token1_symbol"),
                ("decimals", "token1_decimals")
            ],
        )
    )
    return results