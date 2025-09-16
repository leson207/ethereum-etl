import orjson

from src.configs.nats_conn import jetstream, nats_conn
from src.logger import logger


class NatsExporter:
    def __init__(self, stream, subject):
        self.stream = stream
        self.subject = subject
        self.nats_conn = nats_conn
        self.jetstream = jetstream

    async def exist(self):
        stream_info = await self.jetstream.stream_info(self.stream)
        return (
            self.subject in stream_info.state.subjects
            and stream_info.state.subjects[self.subject] > 0
        )

    async def _delete(self):
        await self.jetstream.purge_stream(self.stream, filter=self.subject)
        logger.info(f"Deleted subject {self.subject}")

    async def _create(self):
        logger.info(f"Created subject {self.subject}")

    async def create(self, exist_ok=True):
        if not exist_ok and await self.exist():
            await self._delete()
            await self._create()
        elif exist_ok and not await self.exist():
            await self._create()

    async def export(self, items: list[dict]):
        for item in items:
            try:
                payload = orjson.dumps(item)
            except TypeError:
                import json
                payload = json.dumps(item).encode()

            await self.jetstream.publish(subject=self.subject, payload=payload) # TypeError: Integer exceeds 64-bit range

    async def close(self):
        await self.nats_conn.drain()
