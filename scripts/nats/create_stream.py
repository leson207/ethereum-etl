import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.configs.environment import env


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--foo", action="store_true", help="place holder arg.")
    parser.add_argument("--bar", action="store_true", help="place holder arg.")
    return parser.parse_args()


async def main():
    await connection_manager["jetstream"].add_stream(
        name=env.DATABASE_NAME, subjects=[f"{env.NETWORK}.*"]
    )


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["nats"]))
        runner.run(main())
        runner.run(connection_manager.close())
