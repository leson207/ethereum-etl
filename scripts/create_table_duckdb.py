import argparse
from src.exporters.mapper import EntityExporterMapper
from src.utils.enumeration import ExporterType

def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exporter_type", type=int)
    parser.add_argument("--entity_types", type=str)
    return parser.parse_args()

def main():
    from src.repositories.duckdb.raw_block import RawBlockRepository
    from src.repositories.duckdb.block import BlockRepository
    from src.repositories.duckdb.transaction import TransactionRepository
    from src.repositories.duckdb.withdrawal import WithdrawalRepository

    from src.repositories.duckdb.raw_receipt import RawReceiptRepository
    from src.repositories.duckdb.receipt import ReceiptRepository
    from src.repositories.duckdb.log import LogRepository

    from src.repositories.duckdb.raw_trace import RawTraceRepository
    from src.repositories.duckdb.trace import TraceRepository

    repo_types = [
        # RawBlockRepository,
        # BlockRepository,
        # TransactionRepository,
        # WithdrawalRepository,

        # RawReceiptRepository,
        # ReceiptRepository,
        # LogRepository,

        RawTraceRepository,
        TraceRepository
    ]
    for repo_type in repo_types:
        repo = repo_type()
        repo.create(exist_ok=False, backup=False, restore = False)
            

if __name__ == "__main__":
    main()