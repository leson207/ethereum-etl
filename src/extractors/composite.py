import inspect
from types import SimpleNamespace

from src.logger import logger
from src.utils.enumeration import EntityType
from src.clients.rpc_client import RpcClient
from src.exporters.manager import ExportManager
from src.clients.etherscan_client import EtherscanClient

class CompositeExtractor:
    def __init__(self, exporter: ExportManager, rpc_client: RpcClient, etherscan_client:EtherscanClient):
        self.exporter = exporter
        self.rpc_client = rpc_client
        self.etherscan_client = etherscan_client

        self.mapping = {
            EntityType.RAW_BLOCK: self._extract_raw_block,
            EntityType.BLOCK: self._extract_block,
            EntityType.TRANSACTION: self._extract_transaction,
            EntityType.WITHDRAWAL: self._extract_withdrawal,
            EntityType.RAW_RECEIPT: self._extract_raw_receipt,
            EntityType.RECEIPT: self._extract_receipt,
            EntityType.LOG: self._extract_log,
            EntityType.TRANSFER: self._extract_transfer,
            EntityType.ACCOUNT: self._extract_account,
            EntityType.CONTRACT: self._extract_contract,
            EntityType.ABI: self._extract_abi,
            EntityType.EVENT: self._extract_event,
            EntityType.RAW_TRACE: self._extract_raw_trace,
            EntityType.TRACE: self._extract_trace
        }

    async def run(
        self, start_block, end_block, process_batch_size, request_batch_size, entities
    ):
        for batch_start_block in range(start_block, end_block + 1, process_batch_size):
            batch_end_block = min(batch_start_block + process_batch_size, end_block)

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
        from src.extractors.raw_block import RawBlockExtractor

        raw_block_extractor = RawBlockExtractor(
            exporter=self.exporter, client=self.rpc_client
        )
        await raw_block_extractor.run(
            block_numbers=range(params.batch_start_block, params.batch_end_block+1),
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=params.batch_size,
            show_progress=True,
        )

    def _extract_block(self, params):
        from src.extractors.block import BlockExtractor

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
        from src.extractors.transaction import TransactionExtractor

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
        from src.extractors.withdrawal import WithdrawalExtractor

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
        from src.extractors.raw_receipt import RawReceiptExtractor

        raw_receipt_extractor = RawReceiptExtractor(
            exporter=self.exporter, client=self.rpc_client
        )
        await raw_receipt_extractor.run(
            block_numbers=range(params.batch_start_block, params.batch_end_block+1),
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=params.batch_size,
            show_progress=True,
        )
    
    def _extract_receipt(self, params):
        from src.extractors.receipt import ReceiptExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        receipt_extractor = ReceiptExtractor(exporter=self.exporter)
        receipt_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )

    def _extract_log(self, params):
        from src.extractors.log import LogExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        log_extractor = LogExtractor(exporter=self.exporter)
        log_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
    
    def _extract_transfer(self, params):
        from src.extractors.transfer import TransferExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        transfer_extractor = TransferExtractor(exporter=self.exporter)
        transfer_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
    
    def _extract_account(self, params):
        from src.extractors.account import AccountExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        account_extractor = AccountExtractor(exporter=self.exporter)
        account_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )

    async def _extract_contract(self, params):
        from src.extractors.contract import ContractExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        contract_extractor = ContractExtractor(exporter=self.exporter, client=self.etherscan_client)
        await contract_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
    
    async def _extract_abi(self, params):
        from src.extractors.abi import AbiExtractor

        abi_strings = [contract['abi'] for contract in self.exporter[EntityType.CONTRACT]]
        abi_extractor = AbiExtractor(exporter=self.exporter)
        abi_extractor.run(
            abi_strings,
            initial=0,
            total=len(abi_strings),
            batch_size=1,
            show_progress=True,
        )
    
    def _extract_uniswap_v2_event(self, params):
        from src.extractors.uniswap_v2_event import UniswapV2EventExtractor

        uniswap_v2_event_extractor = UniswapV2EventExtractor(exporter=self.exporter)
        uniswap_v2_event_extractor.run(
            self.exporter[EntityType.LOG],
            batch_size=1,
            show_progress=True,
        )
    
    def _extract_uniswap_v3_event(self, params):
        from src.extractors.uniswap_v3_event import UniswapV3EventExtractor

        uniswap_v3_event_extractor = UniswapV3EventExtractor(exporter=self.exporter)
        uniswap_v3_event_extractor.run(
            self.exporter[EntityType.LOG],
            batch_size=1,
            show_progress=True,
        )
    
    def _extract_event(self, params):
        self._extract_uniswap_v2_event(params=params)
        self._extract_uniswap_v3_event(params=params)
    
    async def _extract_raw_trace(self, params):
        from src.extractors.raw_trace import RawTraceExtractor

        raw_trace_extractor = RawTraceExtractor(
            exporter=self.exporter, client=self.rpc_client
        )
        await raw_trace_extractor.run(
            block_numbers=range(params.batch_start_block, params.batch_end_block+1),
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=5,
            show_progress=True,
        )
    
    async def _extract_trace(self, params):
        from src.extractors.trace import TraceExtractor

        raw_traces = (
            raw_trace["data"] for raw_trace in self.exporter[EntityType.RAW_TRACE]
        )
        trace_extractor = TraceExtractor(exporter=self.exporter)
        trace_extractor.run(
            raw_traces,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )