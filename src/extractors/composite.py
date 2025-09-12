import inspect
from types import SimpleNamespace

from src.clients.binance_client import BinanceClient
from src.clients.etherscan_client import EtherscanClient
from src.clients.rpc_client import RpcClient
from src.exporters.manager import ExportManager
from src.exporters.utils import get_mapper, resolve_dependency
from src.logger import logger
from src.utils.enumeration import EntityType


class CompositeExtractor:
    def __init__(self, target_entity_types: list[str], exporter_types: list[str], rpc_client: RpcClient, etherscan_client:EtherscanClient, binance_client: BinanceClient):
        self.target_entity_types = target_entity_types
        self.require_entity_types = resolve_dependency(self.target_entity_types)
        mapper = get_mapper(self.require_entity_types, exporter_types)
        self.exporter = ExportManager(mapper)
        
        self.rpc_client = rpc_client
        self.etherscan_client = etherscan_client
        self.binance_client = binance_client

        self.mapping = {
            EntityType.RAW_BLOCK: self._extract_raw_block,
            EntityType.BLOCK: self._extract_block,
            EntityType.TRANSACTION: self._extract_transaction,
            EntityType.WITHDRAWAL: self._extract_withdrawal,

            EntityType.RAW_RECEIPT: self._extract_raw_receipt,
            EntityType.RECEIPT: self._extract_receipt,
            EntityType.LOG: self._extract_log,
            EntityType.TRANSFER: self._extract_transfer,
            EntityType.EVENT: self._extract_event,
            EntityType.POOL: self._extract_pool,
            EntityType.TOKEN: self._extract_token,
            EntityType.ACCOUNT: self._extract_account,
            EntityType.CONTRACT: self._extract_contract,
            EntityType.ABI: self._extract_abi,

            EntityType.RAW_TRACE: self._extract_raw_trace,
            EntityType.TRACE: self._extract_trace
        }

    async def run(
        self, start_block, end_block, process_batch_size, request_batch_size
    ):
        for batch_start_block in range(start_block, end_block + 1, process_batch_size):
            batch_end_block = min(batch_start_block + process_batch_size, end_block)

            # processing order follow mapping order
            for entity_type in self.mapping:
                if entity_type not in self.require_entity_types:
                    continue

                extract_fn = self.mapping[entity_type]
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
            
            await self._enrich()

            logger.info(f"Block range: {batch_start_block} - {batch_end_block}")
            for entity_type in self.mapping:
                if entity_type in self.require_entity_types:
                    logger.info(f"Num {entity_type}: {len(self.exporter[entity_type])}")

            self.exporter.exports(self.target_entity_types)
            self.exporter.clear_all()

    async def _enrich(self):
        if EntityType.EVENT in self.target_entity_types:
            await self._enrich_event()

    async def _enrich_event(self):
        from src.extractors.enrich import enrich_event
        enriched_event = await enrich_event(
            self.exporter[EntityType.EVENT],
            self.exporter[EntityType.BLOCK],
            self.exporter[EntityType.POOL],
            self.exporter[EntityType.TOKEN],
        )
        self.exporter.clear(EntityType.EVENT)
        self.exporter.add_items(EntityType.EVENT, enriched_event)

    async def _extract_raw_block(self, params):
        from src.fetchers.raw_block import RawBlockFetcher

        fetcher = RawBlockFetcher(client=self.rpc_client)
        block_numbers = range(params.batch_start_block, params.batch_end_block+1)
        block_data = await fetcher.run(
            block_numbers=block_numbers,
            include_transaction=EntityType.TRANSACTION in self.require_entity_types,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=params.batch_size,
            show_progress=True,
        )
        raw_blocks = [
            {
                "block_number" : block_number,
                "included_transaction": True,
                "data": data 
            }
            for block_number, data in zip(block_numbers, block_data)
        ]
        self.exporter.add_items(EntityType.RAW_BLOCK, raw_blocks)

    async def _extract_block(self, params):
        from src.extractors.block import BlockExtractor

        block_extractor = BlockExtractor()
        raw_blocks = (
            raw_block["data"] for raw_block in self.exporter[EntityType.RAW_BLOCK]
        )
        blocks = block_extractor.run(
            raw_blocks,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        # -------------------------------------------
        from src.fetchers.eth_price import EthPriceFetcher

        timestamps = [block["timestamp"] * 1000 for block in blocks]
        fetcher = EthPriceFetcher(self.binance_client)
        prices = await fetcher.run(timestamps=timestamps)

        for block, price in zip(blocks, prices):
            block['eth_price'] = float(price["p"])

        self.exporter.add_items(EntityType.BLOCK, blocks)

    def _extract_transaction(self, params):
        from src.extractors.transaction import TransactionExtractor

        transaction_extractor = TransactionExtractor()
        raw_blocks = (
            raw_block["data"] for raw_block in self.exporter[EntityType.RAW_BLOCK]
        )
        transactions = transaction_extractor.run(
            raw_blocks,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.TRANSACTION, transactions)

    def _extract_withdrawal(self, params):
        from src.extractors.withdrawal import WithdrawalExtractor

        withdrawal_extractor = WithdrawalExtractor()
        raw_blocks = (
            raw_block["data"] for raw_block in self.exporter[EntityType.RAW_BLOCK]
        )
        withdrawals = withdrawal_extractor.run(
            raw_blocks,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.WITHDRAWAL, withdrawals)
    
    async def _extract_eth_price(self, params):
        from src.fetchers.eth_price import EthPriceFetcher

        fetcher = EthPriceFetcher(client=self.binance_client)
        timestamps=[block["timestamp"]*1000 for block in self.exporter[EntityType.BLOCK]]
        await fetcher.run(
            timestamps=timestamps,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )

    async def _extract_raw_receipt(self, params):
        from src.fetchers.raw_receipt import RawReceiptFetcher

        fetcher = RawReceiptFetcher(client=self.rpc_client)
        block_numbers = range(params.batch_start_block, params.batch_end_block+1)
        receipt_data = await fetcher.run(
            block_numbers=block_numbers,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=params.batch_size,
            show_progress=True,
        )
        raw_receipts = [
            {
                "block_number" : block_number,
                "data": data 
            }
            for block_number, data in zip(block_numbers, receipt_data)
        ]
        self.exporter.add_items(EntityType.RAW_RECEIPT, raw_receipts)
    
    def _extract_receipt(self, params):
        from src.extractors.receipt import ReceiptExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        receipt_extractor = ReceiptExtractor()
        receipts = receipt_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.RECEIPT, receipts)

    def _extract_log(self, params):
        from src.extractors.log import LogExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        log_extractor = LogExtractor()
        logs = log_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.LOG , logs)
    
    def _extract_transfer(self, params):
        from src.extractors.transfer import TransferExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        transfer_extractor = TransferExtractor()
        transfers= transfer_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.TRANSFER, transfers)  
    
    def _extract_uniswap_v2_event(self, params):
        from src.extractors.uniswap_v2_event import UniswapV2EventExtractor

        uniswap_v2_event_extractor = UniswapV2EventExtractor()
        events = uniswap_v2_event_extractor.run(
            self.exporter[EntityType.LOG],
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.EVENT, events)
    
    def _extract_uniswap_v3_event(self, params):
        from src.extractors.uniswap_v3_event import UniswapV3EventExtractor

        uniswap_v3_event_extractor = UniswapV3EventExtractor()
        events = uniswap_v3_event_extractor.run(
            self.exporter[EntityType.LOG],
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.EVENT, events)
    
    def _extract_event(self, params):
        self._extract_uniswap_v2_event(params=params)
        self._extract_uniswap_v3_event(params=params)
    
    def _extract_account(self, params):
        from src.extractors.account import AccountExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        account_extractor = AccountExtractor()
        accounts = account_extractor.run(
            raw_receipts,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.ACCOUNT, accounts)

    async def _extract_contract(self, params):
        from src.extractors.contract import ContractExtractor

        raw_receipts = (
            raw_receipt["data"] for raw_receipt in self.exporter[EntityType.RAW_RECEIPT]
        )
        contract_extractor = ContractExtractor(client=self.etherscan_client)
        contracts = await contract_extractor.run(raw_receipts)
        self.exporter.add_items(EntityType.CONTRACT, contracts)
    
    async def _extract_abi(self, params):
        from src.extractors.abi import AbiExtractor

        abi_strings = [contract['abi'] for contract in self.exporter[EntityType.CONTRACT]]
        abi_extractor = AbiExtractor()
        abis = abi_extractor.run(
            abi_strings,
            initial=0,
            total=len(abi_strings),
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.ABI, abis)

    async def _extract_pool(self, params):
        from src.fetchers.pool import PoolFetcher

        fetcher = PoolFetcher(client=self.rpc_client)
        contract_addresses = [
            contract['pool_address']
            for contract in self.exporter[EntityType.EVENT]
        ]
        contract_addresses = list(set(contract_addresses))
        pool_data = await fetcher.run(
            contract_addresses,
            batch_size=10,
            show_progress=True,
        )
        pools = []
        for address, data in zip(contract_addresses, pool_data):
            if any("error" in i for i in data):
                continue

            token0, token1 = data

            pool = {
                "pool_address": address,
                "token0_address": "0x"+token0["result"][-40:],
                "token1_address": "0x"+token1["result"][-40:]
            }
            pools.append(pool)

        # --------------------------------------------------
        from src.fetchers.balance import BalanceFetcher
        
        fetcher = BalanceFetcher(self.rpc_client)
        balances = await fetcher.run(pools=pools)
        for pool, balance in zip(pools, balances):
            if any("error" in i for i in balance):
                continue

            pool['token0_balance'] = int(balance[0]['result'],16)
            pool['token1_balance'] = int(balance[1]['result'],16)

        self.exporter.add_items(EntityType.POOL, pools)
    
    async def _extract_token(self, params):
        from src.fetchers.token import TokenFetcher
        def decode(hex_string):
            data = bytes.fromhex(hex_string[2:])

            # string length is at byte 32..64
            length = int.from_bytes(data[32:64], "big")

            # string content is at byte 64..64+length
            string_bytes = data[64:64+length]
            decoded = string_bytes.decode()
            return decoded

        token_addresses = [
            addr
            for pool in self.exporter[EntityType.POOL]
            for addr in (pool["token0_address"], pool["token1_address"])
        ]
        token_addresses = list(set(token_addresses))
        fetcher = TokenFetcher(client=self.rpc_client)
        token_data = await fetcher.run(
            token_addresses,
            batch_size=5,
            show_progress=True,
        )

        tokens = []
        for address,data in zip(token_addresses, token_data):
            name, symbol, decimals, total_supply = data
            if any("error" in i for i in data):
                continue
            
            token = {
                "address": address,
                "name": decode(name["result"]),
                "symbol": decode(symbol["result"]),
                "decimals": int(decimals["result"],16),
                "total_supply": int(total_supply["result"], 16),
            }
            tokens.append(token)
        
        self.exporter.add_items(EntityType.TOKEN, tokens)
    
    async def _extract_raw_trace(self, params):
        from src.fetchers.raw_trace import RawTraceFetcher

        fetcher = RawTraceFetcher(client=self.rpc_client)
        block_numbers = range(params.batch_start_block, params.batch_end_block+1)
        trace_data = await fetcher.run(
            block_numbers=block_numbers,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=5,
            show_progress=True,
        )
        raw_traces = [
            {
                "block_number" : block_number,
                "data": data 
            }
            for block_number, data in zip(block_numbers, trace_data)
        ]
        self.exporter.add_items(EntityType.RAW_TRACE, raw_traces)
    
    async def _extract_trace(self, params):
        from src.extractors.trace import TraceExtractor

        raw_traces = (
            raw_trace["data"] for raw_trace in self.exporter[EntityType.RAW_TRACE]
        )
        trace_extractor = TraceExtractor()
        traces = trace_extractor.run(
            raw_traces,
            initial=params.batch_start_block - params.start_block,
            total=params.end_block - params.start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        self.exporter.add_items(EntityType.TRACE, traces)