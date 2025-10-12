import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.configs.environment import env
from src.logger import logger


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, default=env.DATABASE_NAME)
    parser.add_argument("--subjects", types=str, default=f"{env.NETWORK}.*")
    return parser.parse_args()


async def main():
    args = parse_args()
    await connection_manager["jetstream"].add_stream(
        name=args["name"], subjects=args["subjects"].split(",")
    )
    logger.info(f"Create {args['name']} stream with {args['subjects']} subjects!")


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["nats"]))
        runner.run(main())
        runner.run(connection_manager.close())
