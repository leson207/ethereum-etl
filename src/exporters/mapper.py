from src.configs.environment import env
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
            case ExporterType.NATS:
                self.data[entity_type][exporter_type] = self.get_nats(entity_type)
            case ExporterType.CLICKHOUSE:
                self.data[entity_type][exporter_type] = self.get_clickhouse(entity_type)

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
            case EntityType.TRANSFER:
                from src.repositories.sqlite.transfer import TransferRepository

                return TransferRepository()

            case EntityType.EVENT:
                from src.repositories.sqlite.event import EventRepository

                return EventRepository()
            case EntityType.ACCOUNT:
                from src.repositories.sqlite.account import AccountRepository

                return AccountRepository()
            case EntityType.CONTRACT:
                from src.repositories.sqlite.contract import ContractRepository

                return ContractRepository()
            case EntityType.ABI:
                from src.repositories.sqlite.abi import AbiRepository

                return AbiRepository()
            case EntityType.POOL:
                from src.repositories.sqlite.pool import PoolRepository

                return PoolRepository()
            case EntityType.TOKEN:
                from src.repositories.sqlite.token import TokenRepository

                return TokenRepository()
            case EntityType.RAW_TRACE:
                from src.repositories.sqlite.raw_trace import RawTraceRepository

                return RawTraceRepository()
            case EntityType.TRACE:
                from src.repositories.sqlite.trace import TraceRepository

                return TraceRepository()

    def get_clickhouse(self, entity_type):
        match entity_type:
            case EntityType.EVENT:
                from src.repositories.clickhouse.event import EventRepository

                return EventRepository()

    def get_kafka(self, entity_tpye):
        from src.exporters.kafka_exporter import KafkaExporter

        return KafkaExporter(entity_tpye + env.ENVIRONMENT_NAME)

    def get_nats(self, entity_type):
        from src.configs.environment import env
        from src.exporters.nats_exporter import NatsExporter

        return NatsExporter(
            "ETHEREUM" + env.ENVIRONMENT_NAME, f"ethereum.{entity_type}"
        )
