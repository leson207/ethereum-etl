import argparse
import os

from src.configs.environment import env


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--foo", action="store_true", help="place holder arg.")
    parser.add_argument("--bar", action="store_true", help="place holder arg.")
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs("artifacts/dbs", exist_ok=True)
    os.makedirs("artifacts/logs", exist_ok=True)
    os.makedirs("artifacts/data", exist_ok=True)
    os.makedirs("artifacts/debug", exist_ok=True)
    os.makedirs("artifacts/debug/json", exist_ok=True)
    # TODO: Debug function aware of debug variable
    # TODO: create database for clickhouse


if __name__ == "__main__":
    main()
