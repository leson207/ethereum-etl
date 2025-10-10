import argparse
from src.repositories.sqlite.header import sqlite_repo
from src.configs.connection_manager import connection_manager


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", type=str)
    parser.add_argument("--drop", action="store_false")
    parser.add_argument("--backup", action="store_false")
    parser.add_argument("--restore", action="store_false")

    return parser.parse_args()


def main():
    args = parse_args()
    print(args)
    tables = args.tables.split(",")
    if tables[0] == "all":
        tables = list(sqlite_repo.keys())

    for table in tables:
        if table in sqlite_repo:
            repo = sqlite_repo[table]()
            repo.create(drop=args.drop, backup=args.backup, restore=args.restore)


if __name__ == "__main__":
    connection_manager.init_sqlite()
    main()
