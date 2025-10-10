from src.configs.connection_manager import connection_manager
from src.logger import logger


class BaseRepository:
    def __init__(self, stream: str, subject: str):
        self.stream = stream
        self.subject = subject  # like table: accout, event, ...

    async def exist(self):
        stream_info = await connection_manager["jetstream"].stream_info(self.stream)
        return (
            self.subject in stream_info.state.subjects
            and stream_info.state.subjects[self.subject] > 0
        )

    async def drop(self):
        await connection_manager["jetstream"].purge_stream(
            self.stream, filter=self.subject
        )
        logger.info(f"Deleted subject {self.subject}")

    async def _create(self):
        logger.info(f"Created subject {self.subject}")

    async def create(self, drop=True):
        if drop and await self.exist():
            await self.drop()
            await self._create()
        elif not drop and not await self.exist():
            await self._create()
