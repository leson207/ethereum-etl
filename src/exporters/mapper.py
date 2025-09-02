from src.utils.enumeration import EntityType, ExporterType


class EntityExporterMapper:
    def __init__(self):
        self.data = {}
        for entity_type in EntityType.values():
            self.data[entity_type] = {}

    def __getitem__(self, key):
        return self.data[key]

    def set_exporter(self, entity_type, exporter_type):
        match exporter_type:
            case ExporterType.KAFKA:
                self.data[entity_type][exporter_type] = self.get_kafka(entity_type)
            case ExporterType.DUCKDB:
                self.data[entity_type][exporter_type] = self.get_duckdb(entity_type)
            case ExporterType.SQLITE:
                self.data[entity_type][exporter_type] = self.get_sqlite(entity_type)
            case ExporterType.CLICKHOUSE:
                self.data[entity_type][exporter_type] = self.get_clickhouse(entity_type)

    def get_duckdb(self, entity_type):
        match entity_type:
            case EntityType.RAW_BLOCK:
                from src.repositories.duckdb.raw_block import RawBlockRepository
                return RawBlockRepository()
            case EntityType.BLOCK:
                from src.repositories.duckdb.block import BlockRepository
                return BlockRepository()
            case EntityType.TRANSACTION:
                from src.repositories.duckdb.transaction import TransactionRepository
                return TransactionRepository()
            case EntityType.WITHDRAWAL:
                from src.repositories.duckdb.withdrawal import WithdrawalRepository
                return WithdrawalRepository()
            case EntityType.RAW_RECEIPT:
                from src.repositories.duckdb.raw_receipt import RawReceiptRepository
                return RawReceiptRepository()
            case EntityType.RECEIPT:
                from src.repositories.duckdb.receipt import ReceiptRepository
                return ReceiptRepository()
            case EntityType.LOG:
                from src.repositories.duckdb.log import LogRepository
                return LogRepository()
            case EntityType.EVENT:
                return None
            case EntityType.RAW_TRACE:
                from src.repositories.duckdb.raw_trace import RawTraceRepository
                return RawTraceRepository()
            case EntityType.TRACE:
                from src.repositories.duckdb.trace import TraceRepository
                return TraceRepository()
    
    def get_sqlite(self, entity_type):
        match entity_type:
            case EntityType.RAW_BLOCK:
                from src.repositories.sqlite.raw_block import RawBlockRepository
                return RawBlockRepository()
            case EntityType.BLOCK:
                from src.repositories.sqlite.block import BlockRepository
                return BlockRepository()
            case EntityType.TRANSACTION:
                from src.repositories.sqlite.transaction import TransactionRepository
                return TransactionRepository()
            case EntityType.WITHDRAWAL:
                from src.repositories.sqlite.withdrawal import WithdrawalRepository
                return WithdrawalRepository()
            case EntityType.RAW_RECEIPT:
                from src.repositories.sqlite.raw_receipt import RawReceiptRepository
                return RawReceiptRepository()
            case EntityType.RECEIPT:
                from src.repositories.sqlite.receipt import ReceiptRepository
                return ReceiptRepository()
            case EntityType.LOG:
                from src.repositories.sqlite.log import LogRepository
                return LogRepository()
            case EntityType.EVENT:
                return None
            case EntityType.RAW_TRACE:
                from src.repositories.sqlite.raw_trace import RawTraceRepository
                return RawTraceRepository()
            case EntityType.TRACE:
                from src.repositories.sqlite.trace import TraceRepository
                return TraceRepository()

    def get_clickhouse(self, entity_type):
        match entity_type:
            case EntityType.RAW_BLOCK:
                return None
            case EntityType.BLOCK:
                return None
            case EntityType.TRANSACTION:
                return None
            case EntityType.WITHDRAWAL:
                return None
            case EntityType.RAW_RECEIPT:
                return None
            case EntityType.RECEIPT:
                return None
            case EntityType.LOG:
                return None
            case EntityType.EVENT:
                return None
            case EntityType.RAW_TRACE:
                return None
            case EntityType.TRACE:
                return None

    def get_kafka(self, entity_tpye):
        pass
    # + env.DATABASE_SUFFIX
