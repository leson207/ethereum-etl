import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.repositories.clickhouse.header import repo_dict


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entitis", type=str)
    parser.add_argument("--drop", action="store_false")
    parser.add_argument("--backup", action="store_false")
    parser.add_argument("--restore", action="store_false")

    return parser.parse_args()


def main():
    args = parse_args()
    entitis = args.entitis.split(",")
    if entitis[0] == "all":
        entitis = list(repo_dict.keys())

    for entity in entitis:
        if entity in repo_dict:
            repo = repo_dict[entity]()
            repo.create(drop=args.drop, backup=args.backup, restore=args.restore)


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["clcikhouse"]))
        main()
        runner.run(connection_manager.close())
