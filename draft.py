import asyncio

import uvloop

from src.configs.environment import env
from src.exporters.manager import ExporterManager
from src.fetchers.rpc_client import RPCClient
from src.pipeline import block_transaction_witdrawal_pipeline, receipt_log_tokentransfer_pipeline, trace_pipeline
from src.utils.common import dump_json
from src.utils.entity_exporter_mapping import EntityExporterMapper
from src.utils.enumeration import EntityType
from src.tmp import enrich_transactions

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def run_statistic(exporter):
    tmp = exporter.get_item(EntityType.RAW_BLOCK)[0]
    dump_json("draft.json", tmp)

    all_blocks, all_transactions, all_withdrawals = [], [], []
    full_block, full_transaction, full_withdrawal = {}, {}, {}

    for raw_block in exporter.get_item(EntityType.RAW_BLOCK):
        block = raw_block["data"]

        # Collect block
        all_blocks.append(block)
        full_block.update(block)

        # Collect transactions
        for tx in block.get("transactions", []):
            all_transactions.append(tx)
            full_transaction.update(tx)

        # Collect withdrawals
        for wd in block.get("withdrawals", []):
            all_withdrawals.append(wd)
            full_withdrawal.update(wd)

    block_keys = {k for blk in all_blocks for k in blk}
    tx_keys = {k for tx in all_transactions for k in tx}
    wd_keys = {k for wd in all_withdrawals for k in wd}

    print("Schema overview:")
    print(f"- Block keys: {block_keys}")
    print(f"- Transaction keys: {tx_keys}")
    print(f"- Withdrawal keys: {wd_keys}")

    dump_json(env._local_data_folder / "full_block.json", full_block)
    dump_json(env._local_data_folder / "full_transaction.json", full_transaction)
    dump_json(env._local_data_folder / "full_withdrawal.json", full_withdrawal)


async def main():
    mapper = EntityExporterMapper()
    exporter = ExporterManager(mapper)
    exporter.data[EntityType.RAW_BLOCK] = []
    exporter.data[EntityType.RAW_RECEIPT] = []
    exporter.data[EntityType.RAW_TRACE] = []


    client = RPCClient(uri="https://eth-pokt.nodies.app")
    res = await client.send_batch_request([("web3_clientVersion", [])])
    print(f"Web3 Client Version: {res[0]['result']}")

    range_size = 50
    start_blocks = [
        # 0,
        # 23_170_000,
        23_224_053,
    ]
    for start_block in start_blocks:
        end_block = start_block + range_size
        await block_transaction_witdrawal_pipeline(
            client, exporter, start_block, end_block
        )
        await receipt_log_tokentransfer_pipeline(
            client, exporter, start_block, end_block
        )
        # await trace_pipeline(
        #     client, exporter, start_block, end_block
        # )


        tmp = enrich_transactions(exporter.get_item(EntityType.TRANSACTION), exporter.get_item(EntityType.RECEIPT))
        print('8'*100)
        print(len(tmp))

        exporter.clear_all()

    # run_statistic(exporter)

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
