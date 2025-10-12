import argparse
import asyncio

from src.configs.connection_manager import connection_manager


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--foo", action="store_true", help="place holder arg.")
    parser.add_argument("--bar", action="store_true", help="place holder arg.")
    return parser.parse_args()


async def main():
    async with connection_manager["memgraph"].session() as session:
        await session.run("CREATE CONSTRAINT ON (t:TOKEN) ASSERT t.address IS UNIQUE;")
        await session.run("CREATE INDEX ON :TOKEN(address)")
        await session.run("CREATE EDGE INDEX ON :POOL(address)")


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["memgraph"]))
        runner.run(main())
        runner.run(connection_manager.close())
