import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.configs.environment import env
from src.repositories.nats.base import BaseRepository
from src.utils.enumeration import Entity


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities", type=str, default="all")
    parser.add_argument("--drop", action="store_false")

    return parser.parse_args()


async def main():
    args = parse_args()

    entities = args.entities.split(",")
    if entities[0] == "all":
        entities = Entity.values()

    for entity in entities:
        if entity in Entity.values():
            repo = BaseRepository(stream=env.DATABASE_NAME, subject=f"{env.NETWORK}{env.ENVIRONMENT_NAME}.{entity}")
            await repo.create(drop=args.drop)


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["nats"]))
        runner.run(main())
        runner.run(connection_manager.close())
