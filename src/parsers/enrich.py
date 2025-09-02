import polars as pl

# TODO: check field of those join later


def join(
    left, right, left_on, right_on, left_fields: list, right_fields: list, how="left"
):
    def select_and_rename(data, fields):
        df = pl.from_dicts(data, infer_schema_length=None)
        select_cols = []
        rename_map = {}

        for f in fields:
            if isinstance(f, tuple):
                src, dst = f
                select_cols.append(src)
                rename_map[src] = dst
            else:
                select_cols.append(f)

        df = df.select(select_cols)
        if rename_map:
            df = df.rename(rename_map)
        return df

    df_left = select_and_rename(left, left_fields + [left_on])
    df_right = select_and_rename(right, right_fields + [right_on])

    df_joined = df_left.join(df_right, left_on=left_on, right_on=right_on, how=how)

    return df_joined.to_dicts()


def enrich_transactions(transactions, receipts):
    result = join(
        transactions,
        receipts,
        "hash",
        "transaction_hash",
        left_fields=[
            "type",
            "nonce",
            "transaction_index",
            "from_address",
            "to_address",
            "value",
            "gas",
            "gas_price",
            "input",
            "block_number",
            "block_hash",
            "max_fee_per_gas",
            "max_priority_fee_per_gas",
            "max_fee_per_blob_gas",
            "blob_versioned_hashes",
        ],
        right_fields=[
            "cumulative_gas_used",
            "gas_used",
            "contract_address",
            "status",
            "effective_gas_price",
        ],
    )

    return result


def enrich_logs(blocks, logs):
    result = list(
        join(
            logs,
            blocks,
            ("block_number", "number"),
            [
                "type",
                "log_index",
                "transaction_hash",
                "transaction_index",
                "address",
                "data",
                "topics",
                "block_number",
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    if len(result) != len(logs):
        raise ValueError("The number of logs is wrong " + str(result))

    return result


def enrich_events(events, blocks, transactions):
    result = list(
        join(
            events,
            blocks,
            "block_number",
            "number",
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
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    result = list(
        join(
            result,
            transactions,
            "transaction_hash",
            "hash",
            [
                "type",
                "pool_address",
                "log_index",
                "block_number",
                "dex",
                "amount0_in",
                "amount1_in",
                "amount0_out",
                "amount1_out",
                "block_timestamp",
                "block_hash",
            ],
            [
                ("from_address", "sender"),
                ("to_address", "to"),
            ],
        )
    )

    return result


def enrich_traces(blocks, traces):
    result = list(
        join(
            traces,
            blocks,
            ("block_number", "number"),
            [
                "type",
                "transaction_index",
                "from_address",
                "to_address",
                "value",
                "input",
                "output",
                "trace_type",
                "call_type",
                "reward_type",
                "gas",
                "gas_used",
                "subtraces",
                "trace_address",
                "error",
                "status",
                "transaction_hash",
                "block_number",
                "trace_id",
                "trace_index",
            ],
            [
                ("timestamp", "block_timestamp"),
                ("hash", "block_hash"),
            ],
        )
    )

    return result
