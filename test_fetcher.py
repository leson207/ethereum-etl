import asyncio


async def test_raw_block_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.raw_block import RawBlockFetcher

    # url = "https://mainnet.infura.io/v3/29b89f1d2c8347d291a17088b2bf2a52"
    url = "https://eth-pokt.nodies.app"
    client = RpcClient(url)
    fetcher = RawBlockFetcher(client=client)
    blocks = await fetcher.run(range(23_000_000, 23_000_010))
    for block in blocks:
        print(block.keys())

async def test_raw_receipt_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.raw_receipt import RawReceiptFetcher

    # url = "https://mainnet.infura.io/v3/29b89f1d2c8347d291a17088b2bf2a52"
    url = "https://eth-pokt.nodies.app"
    client = RpcClient(url)
    fetcher = RawReceiptFetcher(client=client)
    all_block_receipts = await fetcher.run(range(23_000_000, 23_000_010))
    for block_receipts in all_block_receipts:
        print(len(block_receipts))
        print(block_receipts[0].keys())

async def test_raw_trace_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.raw_trace import RawTraceFetcher

    # url = "https://mainnet.infura.io/v3/29b89f1d2c8347d291a17088b2bf2a52"
    url = "https://eth-pokt.nodies.app"
    client = RpcClient(url)
    fetcher = RawTraceFetcher(client=client)
    all_block_traces = await fetcher.run(range(23_000_000, 23_000_010))
    for block_traces in all_block_traces:
        print(len(block_traces))
        print(block_traces[0].keys())

async def test_pool_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.pool import PoolFetcher

    # url = "https://mainnet.infura.io/v3/29b89f1d2c8347d291a17088b2bf2a52"
    url = "https://eth-pokt.nodies.app"
    client = RpcClient(url)
    fetcher = PoolFetcher(client=client)
    res = await fetcher.run(["0xe55b01ca3d407cd4de38e988c84ec13ae188b0ca"]*10)
    for i in res:
        print(i)

async def test_token_fetcher():
    from src.clients.rpc_client import RpcClient
    from src.fetchers.token import TokenFetcher

    # url = "https://mainnet.infura.io/v3/29b89f1d2c8347d291a17088b2bf2a52"
    url = "https://eth-pokt.nodies.app"
    client = RpcClient(url)
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

if __name__ == "__main__":
    # asyncio.run(test_raw_block_fetcher())
    # asyncio.run(test_raw_receipt_fetcher())
    # asyncio.run(test_raw_trace_fetcher())
    asyncio.run(test_pool_fetcher())
    # asyncio.run(test_token_fetcher())
    # asyncio.run(test_eth_price_fetcher())
    # asyncio.run(test_signature_fetcher())
