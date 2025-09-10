# from src.fetchers.raw_block import RawBlockFetcher
# from src.fetchers.rpc_client import RPCClient
# import asyncio
# def form_request(params):
#         requests = [
#             (
#                 "eth_getBlockByNumber",
#                 [hex(param["block_number"]), param["included_transaction"]],
#             )
#             for param in params
#         ]
#         return requests

# async def main():
#     client = RPCClient(uri="https://eth-pokt.nodies.app")
#     params = [
#          {
#               "block_number": i,
#               "included_transaction": False
#          }
#          for i in range(0, 29)
#     ]
#     requests = form_request(params)
#     await client.send_batch_request(requests)


# if __name__ == "__main__":
#     asyncio.run(main())
from processors.raw_block_fetcher import RawBlockFetcher
from src.clients.rpc_client import RpcClient
from src.clients.retry import RpcBatchSender
import asyncio

async def main():
    client = RpcClient(uri="https://eth-pokt.nodies.app")
    sender = RpcBatchSender(client)
    fetcher = RawBlockFetcher(sender)
    await fetcher.run(range(0,1000))


if __name__ == "__main__":
    asyncio.run(main())