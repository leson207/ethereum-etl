import asyncio
from src.configs.environment import env

async def test_raw_block_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.raw_block import RawBlockFetcher

    client = RpcClient(env.PROVIDER_URIS)
    fetcher = RawBlockFetcher(client=client)
    blocks = await fetcher.run(range(23_000_000, 23_000_010))
    for block in blocks:
        print(block.keys())

async def test_raw_receipt_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.raw_receipt import RawReceiptFetcher

    client = RpcClient(env.PROVIDER_URIS)
    fetcher = RawReceiptFetcher(client=client)
    all_block_receipts = await fetcher.run(range(23_000_000, 23_000_010))
    for block_receipts in all_block_receipts:
        print(len(block_receipts))
        print(block_receipts[0].keys())

async def test_raw_trace_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.raw_trace import RawTraceFetcher

    client = RpcClient(env.PROVIDER_URIS)
    fetcher = RawTraceFetcher(client=client)
    all_block_traces = await fetcher.run(range(23_000_000, 23_000_010))
    for block_traces in all_block_traces:
        print(len(block_traces))
        print(block_traces[0].keys())

async def test_pool_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.pool import PoolFetcher

    client = RpcClient(env.PROVIDER_URIS)
    fetcher = PoolFetcher(client=client)
    res = await fetcher.run(["0xe55b01ca3d407cd4de38e988c84ec13ae188b0ca"]*10)
    for i in res:
        print(i)

async def test_token_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.token import TokenFetcher

    client = RpcClient(env.PROVIDER_URIS)
    fetcher = TokenFetcher(client=client)
    res = await fetcher.run(["0x57e114B691Db790C35207b2e685D4A43181e6061"]*10)
    for i in res:
        print(i)

async def test_eth_price_fetcher():
    from src.clients.binance_client import BinanceClient
    from src.fetchers.eth_price import EthPriceFetcher

    url = "https://api4.binance.com/api/v3"
    client = BinanceClient(url)
    fetcher = EthPriceFetcher(client=client)
    res = await fetcher.run([1757564269000] * 10)
    for i in res:
        print(i)


async def test_signature_fetcher():
    from src.clients.four_byte import FourByteClient
    from src.fetchers.event_signature import EventSignatureFetcher

    url = "https://www.4byte.directory/api/v1"
    client = FourByteClient(url)
    fetcher = EventSignatureFetcher(client=client)
    res = await fetcher.run(
        ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] * 10
    )
    for i in res:
        print(i)

async def test_contract_fetcher():
    from src.clients.etherscan_client import EtherscanClient
    from src.fetchers.contract import ContractFetcher

    url = "https://api.etherscan.io/v2/api"
    client = EtherscanClient(url)
    fetcher = ContractFetcher(client=client)
    res = await fetcher.run(
        ["0xc3db44adc1fcdfd5671f555236eae49f4a8eea18"] * 10
    )
    for i in res:
        print(i.keys())

async def test_balance_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.balance import BalanceFetcher

    client = RpcClient(env.PROVIDER_URIS)
    fetcher = BalanceFetcher(client=client)
    pool = {
        "pool_address": "0x9E1bF6Db42E4C14a28dd484655ba80EBC38DFb5D",
        "token0_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "token1_address": "0x1963A95cfc30e49Cc75F7F2de6027289971cAc79"
    }
    res = await fetcher.run([pool]*10)
    for i in res:
        print(i)

if __name__ == "__main__":
    # asyncio.run(test_raw_block_fetcher())
    # asyncio.run(test_raw_receipt_fetcher())
    # asyncio.run(test_raw_trace_fetcher())
    # asyncio.run(test_pool_fetcher())
    # asyncio.run(test_token_fetcher())
    # asyncio.run(test_eth_price_fetcher())
    # asyncio.run(test_signature_fetcher())
    # asyncio.run(test_contract_fetcher())
    asyncio.run(test_balance_fetcher())
