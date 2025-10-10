import argparse
from src.repositories.sqlite.header import sqlite_repo
from src.configs.connection_manager import connection_manager


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", type=str)
    parser.add_argument("--backup", default=False, action="store_true")
    parser.add_argument("--delete-backup", default=False, action="store_true")

    return parser.parse_args()


def main():
    args = parse_args()
    tables = args.tables.split(",")
    if tables[0] == "all":
        tables = list(sqlite_repo.keys())

    for table in tables:
        if table in sqlite_repo:
            repo = sqlite_repo[table]()
            repo.delete(backup=args.backup)
            if args.delete_backup:
                repo._drop_backup()


if __name__ == "__main__":
    connection_manager.init_sqlite()
    main()
