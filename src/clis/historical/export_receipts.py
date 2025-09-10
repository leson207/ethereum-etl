import argparse
import asyncio

import uvloop

from src.utils.enumeration import EntityType

from src.exporters.manager import ExportManager

from src.clis.utils import get_mapper

from src.parsers.transfer_parser import TransferParser
from src.parsers.contract_parser import ContractParser
from src.parsers.abi_parser import ABIParser
from src.parsers.account_parser import AccountParser
from src.extractors.pool import PoolExtractor
from src.clients.rpc_client import RpcClient
from src.extractors.token import TokenExtractor
from src.extractors.raw_receipt import RawReceiptExtractor
from src.extractors.receipt import ReceiptExtractor
from src.extractors.log import LogExtractor
from src.extractors.transfer import TransferExtractor
from src.extractors.contract import ContractExtractor
from src.extractors.abi import AbiExtractor
from src.extractors.uniswap_v2_event import UniswapV2EventExtractor
from src.extractors.uniswap_v3_event import UniswapV3EventExtractor
from src.logger import logger

from src.clients.etherscan_client import EtherscanClient
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--process-batch-size", type=int, default=1000)
    parser.add_argument("--request-batch-size", type=int, default=30)
    parser.add_argument("--entity_types", type=str, default="")
    parser.add_argument("--exporter_types", type=str, default="")
    return parser.parse_args()


async def main(start_block, end_block, process_batch_size, request_batch_size, entity_types, exporter_types):
    mapper = get_mapper(entity_types, exporter_types)
    exporter = ExportManager(mapper)

    # client = RPCClient(uri="https://eth-pokt.nodies.app")
    # res = await client.send_batch_request([("web3_clientVersion", [])])
    # logger.info(f"Web3 Client Version: {res[0]['result']}")

    # fetcher = RawReceiptFetcher(client=client, exporter=exporter)

    rpc_client = RpcClient(uri="https://eth-pokt.nodies.app")
    etherscan_client = EtherscanClient(url="https://api.etherscan.io/v2/api")
    res = await rpc_client.send([("web3_clientVersion", [])])
    logger.info(f"Web3 Client Version: {res[0]['result']}")

    raw_receipt_extractor = RawReceiptExtractor(exporter=exporter, client=rpc_client)
    receipt_extractor = ReceiptExtractor(exporter=exporter)
    log_extractor = LogExtractor(exporter=exporter)
    transfer_extractor = TransferExtractor(exporter=exporter)
    contract_extractor = ContractExtractor(exporter=exporter, client=etherscan_client)
    abi_extractor = AbiExtractor(exporter=exporter)
    uniswap_v2_event_extractor = UniswapV2EventExtractor(exporter=exporter)
    uniswap_v3_event_extractor = UniswapV3EventExtractor(exporter=exporter)
    pool_extractor = PoolExtractor(exporter=exporter, client=rpc_client) # TODO: rewatch here
    token_extractor = TokenExtractor(exporter=exporter, client=rpc_client) # TODO: rewatch here

    # pool_fetcher = PoolFetcher(sender=sender, exporter=exporter)
    # token_fetcher = TokenFetcher(sender=sender, exporter=exporter)

    # receipt_parser = ReceiptParser(exporter=exporter)
    # log_parser = LogParser(exporter=exporter)
    # transfer_parser = TransferParser(exporter=exporter)
    # contract_parser = ContractParser(exporter=exporter)
    # abi_parser = ABIParser(exporter=exporter)
    # account_parser = AccountParser(exporter=exporter)

    for batch_start_block in range(start_block, end_block + 1, process_batch_size):
        batch_end_block = min(batch_start_block + process_batch_size, end_block + 1)

        await raw_receipt_extractor.run(
            block_numbers=range(batch_start_block, batch_end_block),
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=request_batch_size,
            show_progress=True,
        )

        raw_receipts = [
            raw_receipt["data"] for raw_receipt in exporter[EntityType.RAW_RECEIPT]
        ]
        receipt_extractor.run(
            raw_receipts,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        log_extractor.run(
            raw_receipts,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        transfer_extractor.run(
            raw_receipts,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        # account_parser.parse(
        #     raw_receipts,
        #     initial=batch_start_block - start_block,
        #     total=end_block - start_block + 1,
        #     batch_size=1,
        #     show_progress=True,
        # )
        await contract_extractor.run(
            raw_receipts,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        abi_strings = [contract['abi'] for contract in exporter[EntityType.CONTRACT]]
        abi_extractor.run(
            abi_strings,
            initial=0,
            total=len(abi_strings),
            batch_size=1,
            show_progress=True,
        )
        uniswap_v2_event_extractor.run(
            exporter[EntityType.LOG],
            batch_size=1,
            show_progress=True
        )
        uniswap_v3_event_extractor.run(
            exporter[EntityType.LOG],
            batch_size=1,
            show_progress=True
        )

        contract_addresses = [contract['address'] for contract in exporter[EntityType.CONTRACT]]
        await pool_extractor.run(
            contract_addresses,
            batch_size=10,
            show_progress=True,
        )

        token_addresses = [
            addr
            for pool in exporter[EntityType.POOL]
            for addr in (pool["token0_address"], pool["token1_address"])
        ]
        # for pool in exporter[EntityType.POOL]:
        #     if "0x000000000000000000000000a250cc729bb3323e" in pool["token0_address"].lower():
        #         print('8'*100)
        #         print(pool)
            
        #     if "0x000000000000000000000000a250cc729bb3323e" in pool["token1_address"].lower():
        #         print('8'*100)
        #         print(pool)

        await token_extractor.run(
            token_addresses,
            batch_size=5,
            show_progress=True,
        )


        logger.info(f"Block range: {batch_start_block} - {batch_end_block}")
        logger.info(f"Num RawReceipt: {len(exporter[EntityType.RAW_RECEIPT])}")
        logger.info(f"Num Receipt: {len(exporter[EntityType.RECEIPT])}")
        logger.info(f"Num Log: {len(exporter[EntityType.LOG])}")
        logger.info(f"Num Transfer: {len(exporter[EntityType.TRANSFER])}")
        logger.info(f"Num Account: {len(exporter[EntityType.ACCOUNT_ADDRESS])}")
        logger.info(f"Num Contract: {len(exporter[EntityType.CONTRACT_ADDRESS])}")
        logger.info(f"Num ABI: {len(exporter[EntityType.ABI])}")
        logger.info(f"Num pool: {len(exporter[EntityType.POOL])}")
        logger.info(f"Num Token: {len(exporter[EntityType.TOKEN])}")

        exporter.export_all()
        exporter.clear_all()

    await rpc_client.close()
    await etherscan_client.close()


if __name__ == "__main__":
    args = parse_arg()
    entity_types = args.entity_types.split(',')
    exporter_types = args.exporter_types.split(',')
    asyncio.run(
        main(
            args.start_block,
            args.end_block,
            args.process_batch_size,
            args.request_batch_size,
            entity_types,
            exporter_types
        )
    )

# python -m src.clis.historical.export_receipts --start-block 23170000 --end-block 23170030 \
#     --process-batch-size 100 --request-batch-size 30 \
# --entity_types raw_receipt,receipt,log --exporter_types duckdb