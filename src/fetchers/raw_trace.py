from src.fetchers.base import BaseFetcher
from src.schemas.python.raw_trace import RawTrace


class RawTraceFetcher(BaseFetcher):
    def _from_request(self, params):
        requests = [
            (
                "trace_block",
                [hex(param["block_number"])],
            )
            for param in params
        ]
        return requests

    async def fetch(self, requests):
        response = await self.client.send_batch_request(requests)

        raws = []
        for request, data in zip(requests, response):
            raw = RawTrace(
                block_number=int(request[1][0], 16),
                data=data["result"],
            )
            raws.append(raw)

        return raws

