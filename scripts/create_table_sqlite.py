import argparse
from src.exporters.mapper import EntityExporterMapper
from src.utils.enumeration import ExporterType

def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exporter_type", type=int)
    parser.add_argument("--entity_types", type=str)
    return parser.parse_args()

def main():
    from src.repositories.sqlite.raw_block import RawBlockRepository
    from src.repositories.sqlite.block import BlockRepository
    from src.repositories.sqlite.transaction import TransactionRepository
    from src.repositories.sqlite.withdrawal import WithdrawalRepository

    from src.repositories.sqlite.raw_receipt import RawReceiptRepository
    from src.repositories.sqlite.receipt import ReceiptRepository
    from src.repositories.sqlite.log import LogRepository

    from src.repositories.sqlite.raw_trace import RawTraceRepository
    from src.repositories.sqlite.trace import TraceRepository

    repo_types = [
        RawBlockRepository,
        BlockRepository,
        TransactionRepository,
        WithdrawalRepository,

        RawReceiptRepository,
        ReceiptRepository,
        LogRepository,

        RawTraceRepository,
        TraceRepository
    ]
    for repo_type in repo_types:
        repo = repo_type()
        repo.create(exist_ok=False, backup=False, restore = False)
            

if __name__ == "__main__":
    main()