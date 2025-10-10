import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.logger import logger
from src.utils.enumeration import Entity

# TODO: finish this

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entitis", type=str)
    parser.add_argument("--drop", action="store_false")

    return parser.parse_args()


class Repo:
    def __init__(self, stream, subject):
        self.stream = stream  # like database name: here ethereum
        self.subject = subject  # like table: accout, event, ...

    async def exist(self):
        stream_info = await connection_manager["jetstream"].stream_info(self.stream)
        return (
            self.subject in stream_info.state.subjects
            and stream_info.state.subjects[self.subject] > 0
        )

    async def _drop(self):
        await connection_manager["jetstream"].purge_stream(
            self.stream, filter=self.subject
        )
        logger.info(f"Deleted subject {self.subject}")

    async def _create(self):
        logger.info(f"Created subject {self.subject}")

    async def create(self, drop=True):
        if drop and await self.exist():
            await self._drop()
            await self._create()
        elif not drop and not await self.exist():
            await self._create()


def main():
    args = parse_args()

    entitis = args.entitis.split(",")
    if entitis[0] == "all":
        entitis = Entity.values()

    for entity in entitis:
        if entity in Entity.values():
            if args.drop:
                pass

            repo = repo_dict[entity]()
            repo.create(drop=args.drop, backup=args.backup, restore=args.restore)


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["nats"]))
        main()
        runner.run(connection_manager.close())
