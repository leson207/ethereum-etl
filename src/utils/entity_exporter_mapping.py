from collections import defaultdict

from src.repositories.duckdb.block import BlockRepository
from src.repositories.duckdb.raw_block import RawBlockRepository
from src.repositories.duckdb.transaction import TransactionRepository
from src.repositories.duckdb.withdrawal import WithdrawalRepository
from src.utils.enumeration import EntityType, ExporterType


# TODO: re-design here
class EntityExporterMapper:
    def __init__(self):
        self.data = defaultdict(dict)  # [entity_type][exporter_type]
        self.init_funcs = {ExporterType.DUCKDB: self._init_duckdb}

    def _init_duckdb(self, entity_type: str):
        match entity_type:
            case EntityType.RAW_BLOCK:
                repo = RawBlockRepository()
            case EntityType.BLOCK:
                repo = BlockRepository()
            case EntityType.TRANSACTION:
                repo = TransactionRepository()
            case EntityType.WITHDRAWAL:
                repo = WithdrawalRepository()
            case _:
                raise ValueError(f"Unknown entity type: {entity_type}")

        self.data[entity_type][ExporterType.DUCKDB] = repo

    def __getitem__(self, key):
        return self.data[key]

    def set(self, entity_type, exporter_type):
        self.init_funcs[exporter_type](entity_type)
