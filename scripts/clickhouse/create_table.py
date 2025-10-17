import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.repositories.clickhouse.header import repo_dict


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities", type=str, default="all")
    parser.add_argument("--drop", default=False, action="store_true")
    parser.add_argument("--backup", default=False, action="store_true")
    parser.add_argument("--restore", default=False, action="store_true")

    return parser.parse_args()


def main():
    args = parse_args()

    entities = args.entities.split(",")
    if entities[0] == "all":
        entities = list(repo_dict.keys())

    for entity in entities:
        if entity in repo_dict:
            repo = repo_dict[entity]()
            repo.create(drop=args.drop, backup=args.backup, restore=args.restore)


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["clickhouse"]))
        main()
        runner.run(connection_manager.close())
