from src.fetchers.base import BaseFetcher
from src.schemas.python.raw_receipt import RawReceipt


class RawReceiptFetcher(BaseFetcher):
    def _from_request(self, params):
        requests = [
            (
                "eth_getBlockReceipts",
                [hex(param["block_number"])],
            )
            for param in params
        ]
        return requests

    async def fetch(self, requests):
        response = await self.client.send_batch_request(requests)

        raws = []
        for request, data in zip(requests, response):
            raw = RawReceipt(
                block_number=int(request[1][0], 16),
                data=data["result"],
            )
            raws.append(raw)

        return raws

