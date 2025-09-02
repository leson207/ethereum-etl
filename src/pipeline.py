import asyncio

import uvloop

from src.extractors.block import BlockExtractor
from src.extractors.receipt import ReceiptExtractor
from src.extractors.trace import TraceExtractor
from src.parsers.block_parser import BlockParser
from parsers.raw_block_parser import (
    BlockTransactionWithdrawalParser,
)
from src.parsers.log_parser import LogParser
from parsers.raw_receipt_parser import ReceiptLogParser
from src.parsers.receipt_parser import ReceiptParser
from parsers.raw_trace_parser import TraceParser
from src.parsers.transaction_parser import TransactionParser
from src.parsers.withdrawal_parser import WithdrawalParser
from src.utils.enumeration import EntityType
from src.parsers.token_transfer_parser import TokenTransferParser
from parsers.event_parser import SwapExtractor
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def block_transaction_witdrawal_pipeline(
    client, exporter, start_block, end_block, initial = None, total = None
):
    extractor = BlockExtractor(
        client=client, exporter=exporter.data[EntityType.RAW_BLOCK]
    )

    block_parser = BlockParser(exporter)
    transaction_parser = TransactionParser(exporter)
    withdrawal_parser = WithdrawalParser(exporter)

    block_transaction_withdrawal_parser = BlockTransactionWithdrawalParser(
        block_parser=block_parser,
        transaction_parser=transaction_parser,
        withdrawal_parser=withdrawal_parser,
    )

    params = [
        {"block_number": i, "included_transaction": True}
        for i in range(start_block, end_block + 1)
    ]

    await extractor.run(
        params=params,
        initial=initial or 0,
        total=total or len(params),
        batch_size=30,
        show_progress=True,
    )
    raw_blocks = [
        raw_block["data"] for raw_block in exporter.get_item(EntityType.RAW_BLOCK)
    ]
    block_transaction_withdrawal_parser.parse(raw_blocks)

    print("Pipeline completed:")
    print(f"- Raw blocks: {len(exporter.get_item(EntityType.RAW_BLOCK))}")
    print(f"- Blocks: {len(exporter.get_item(EntityType.BLOCK))}")
    print(f"- Transactions: {len(exporter.get_item(EntityType.TRANSACTION))}")
    print(f"- Withdrawals: {len(exporter.get_item(EntityType.WITHDRAWAL))}")


async def receipt_log_tokentransfer_pipeline(client, exporter, start_block, end_block):
    extractor = ReceiptExtractor(client, exporter.data[EntityType.RAW_RECEIPT])

    receipt_parser = ReceiptParser(exporter)
    log_parser = LogParser(exporter)
    token_transfer_parser = TokenTransferParser(exporter)
    swap_parser = SwapExtractor(exporter)

    receipt_log_parser = ReceiptLogParser(
        receipt_parser=receipt_parser, log_parser=log_parser
    )

    params = [{"block_number": i} for i in range(start_block, end_block + 1)]
    await extractor.run(
        params=params,
        initial=0,
        total=len(params),
        batch_size=30,
        show_progress=True,
    )
    raw_receipts = []
    for raw_receipt in exporter.get_item(EntityType.RAW_RECEIPT):
        raw_receipts.extend(raw_receipt["data"])

    receipt_log_parser.parse(
        raw_receipts,
        initial=0,
        total=len(raw_receipts),
        batch_size=1,
        show_progress=True,
    )

    logs = exporter.get_item(EntityType.LOG)
    token_transfer_parser.parse(
        logs,
        initial=0,
        total=len(logs),
        batch_size=1,
        show_progress=True
    )

    swap_parser.extract(
        logs,
        initial=0,
        total=len(logs),
        batch_size=1,
        show_progress=True
    )

    print("Pipeline completed:")
    print(f"- Raw receipts: {len(exporter.get_item(EntityType.RAW_RECEIPT))}")
    print(f"- Receipts: {len(exporter.get_item(EntityType.RECEIPT))}")
    print(f"- Logs: {len(exporter.get_item(EntityType.LOG))}")
    print(f"- TokenTransfer: {len(exporter.get_item(EntityType.TOKEN_TRANSFER))}")
    print(f"- Swap: {len(exporter.get_item(EntityType.SWAP))}")


async def trace_pipeline(client, exporter, start_block, end_block):
    extractor = TraceExtractor(client, exporter.data[EntityType.RAW_TRACE])

    trace_parser = TraceParser(exporter)

    params = [{"block_number": i} for i in range(start_block, end_block + 1)]
    await extractor.run(
        params=params,
        initial=0,
        total=len(params),
        batch_size=5,
        show_progress=True,
    )
    raw_traces = []
    for raw_trace in exporter.get_item(EntityType.RAW_TRACE):
        raw_traces.extend(raw_trace["data"])

    trace_parser.parse(
        raw_traces,
        initial=0,
        total=len(raw_traces),
        batch_size=1,
        show_progress=True,
    )

    print("Pipeline completed:")
    print(f"- Raw traces: {len(exporter.get_item(EntityType.RAW_TRACE))}")
    print(f"- Traces: {len(exporter.get_item(EntityType.TRACE))}")
