import inspect
from types import SimpleNamespace

from src.logger import logger
from src.utils.enumeration import EntityType
from src.clients.rpc_client import RpcClient
from src.exporters.manager import ExportManager
from src.extractors.block import BlockExtractor
from src.extractors.raw_block import RawBlockExtractor
from src.extractors.transaction import TransactionExtractor
from src.extractors.withdrawal import WithdrawalExtractor

class CompositeExtractor:
    def __init__(self, exporter: ExportManager, rpc_client: RpcClient):
        self.exporter = exporter
        self.rpc_client = rpc_client

        self.mapping = {
            EntityType.RAW_BLOCK: self._extract_raw_block,
            EntityType.BLOCK: self._extract_block,
            EntityType.RAW_RECEIPT: self._extract_raw_receipt,
        }

    async def run(
        self, start_block, end_block, process_batch_size, request_batch_size, entities
    ):
        for batch_start_block in range(start_block, end_block + 1, process_batch_size):
            batch_end_block = min(batch_start_block + process_batch_size, end_block + 1)

            for entity in entities:
                extract_fn = self.mapping[entity]
                params = SimpleNamespace(
                    start_block=start_block,
                    end_block=end_block,
                    batch_start_block=batch_start_block,
                    batch_end_block=batch_end_block,
                    batch_size=request_batch_size,
                )
                result = extract_fn(params)
                if inspect.isawaitable(result):
                    await result

            logger.info(f"Block range: {batch_start_block} - {batch_end_block}")
            for entity in entities:
                logger.info(f"Num {entity}: {len(self.exporter[entity])}")

            self.exporter.export_all()
            self.exporter.clear_all()

    async def _extract_raw_block(self, params):
        raw_block_extractor = RawBlockExtractor(
            exporter=self.exporter, client=self.rpc_client
        )
        await raw_block_extractor.run(
            block_numbers=range(params.batch_start_block, params.batch_end_block),
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=params.batch_size,
            show_progress=True,
        )

    def _extract_block(self, params):
        block_extractor = BlockExtractor(exporter=self.exporter)
        raw_blocks = (
            raw_block["data"] for raw_block in self.exporter[EntityType.RAW_BLOCK]
        )
        block_extractor.run(
            raw_blocks,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )

    def _extract_transaction(self, params):
        transaction_extractor = TransactionExtractor(exporter=self.exporter)
        raw_blocks = (
            raw_block["data"] for raw_block in self.exporter[EntityType.RAW_BLOCK]
        )
        transaction_extractor.run(
            raw_blocks,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )

    def _extract_withdrawal(self, params):
        withdrawal_extractor = WithdrawalExtractor(exporter=self.exporter)
        raw_blocks = (
            raw_block["data"] for raw_block in self.exporter[EntityType.RAW_BLOCK]
        )
        withdrawal_extractor.run(
            raw_blocks,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )

    async def _extract_raw_receipt(self, params):
        raw_receipt_extractor = RawReceiptExtractor(
            exporter=self.exporter, client=self.rpc_client
        )
        await raw_receipt_extractor.run(
            block_numbers=range(params.batch_start_block, params.batch_end_block),
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=params.batch_size,
            show_progress=True,
        )
