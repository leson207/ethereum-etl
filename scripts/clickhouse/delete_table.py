import argparse
import asyncio

from src.configs.connection_manager import connection_manager
from src.repositories.clickhouse.header import repo_dict


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entities", type=str)
    parser.add_argument("--backup", default=False, action="store_true")
    parser.add_argument("--delete-backup", default=False, action="store_true")

    return parser.parse_args()


def main():
    args = parse_args()

    entities = args.entities.split(",")
    if entities[0] == "all":
        entities = list(repo_dict.keys())

    for entity in entities:
        if entity in repo_dict:
            repo = repo_dict[entity]()
            repo.delete(backup=args.backup)
            if args.delete_backup:
                repo._drop_backup()


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init(["sqlite"]))
        main()
        runner.run(connection_manager.close())
