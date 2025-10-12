import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.configs.environment import env
from src.logger import logger

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, default=env.DATABASE_NAME)
    return parser.parse_args()


async def main():
    args = parse_args()
    await connection_manager["jetstream"].delete_stream(args["name"])
    logger.info(f"Drop {args['name']} stream!")

if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["nats"]))
        runner.run(main())
        runner.run(connection_manager.close())
