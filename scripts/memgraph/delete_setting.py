import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.logger import logger

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--foo", action="store_true", help="place holder arg.")
    parser.add_argument("--bar", action="store_true", help="place holder arg.")
    return parser.parse_args()


async def main():
    async with connection_manager["memgraph"].session() as session:
        await session.run("DROP ALL INDEXES")
        logger.info("Drop all existing index!")

        await session.run("DROP ALL CONSTRAINTS")
        logger.info("Drop all existing constraint!")

        await session.run("MATCH (n) DETACH DELETE n")
        logger.info("Drop all existing node!")

        # -----------------------------------------------------

        await session.run("DROP CONSTRAINT ON (t:TOKEN) ASSERT t.address IS UNIQUE")
        logger.info("Drop node unique constraint for TOKEN!")

        await session.run("DROP INDEX ON :TOKEN(address)")
        logger.info("Drop node index for TOKEN.address!")

        await session.run("DROP EDGE INDEX ON: POOL(address)")
        logger.info("Drop edge index for POOL.address!")


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["memgraph"]))
        runner.run(main())
        runner.run(connection_manager.close())
