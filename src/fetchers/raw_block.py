from src.fetchers.base import BaseFetcher
from src.schemas.python.raw_block import RawBlock


class RawBlockFetcher(BaseFetcher):
    def _from_request(self, params):
        requests = [
            (
                "eth_getBlockByNumber",
                [hex(param["block_number"]), param["included_transaction"]],
            )
            for param in params
        ]
        return requests

    async def fetch(self, requests):
        response = await self.client.send_batch_request(requests)

        raws = []
        for request, data in zip(requests, response):
            raw = RawBlock(
                block_number=int(request[1][0], 16),
                included_transaction=request[1][1],
                data=data["result"],
            )
            raws.append(raw)

        return raws
