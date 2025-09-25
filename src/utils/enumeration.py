class Entity:
    RAW_BLOCK = "raw_block"
    BLOCK = "block"
    TRANSACTION = "transaction"
    WITHDRAWAL = "withdrawal"

    RAW_RECEIPT = "raw_receipt"
    RECEIPT = "receipt"
    LOG = "log"
    TRANSFER = "transfer"
    EVENT = "event"
    ACCOUNT = "account"
    CONTRACT = "contract"
    ABI = "abi"
    POOL = "pool"
    TOKEN = "token"

    RAW_TRACE = "raw_trace"
    TRACE = "trace"

    @classmethod
    def values(cls):
        return [
            v
            for k, v in cls.__dict__.items()
            if not k.startswith("__") and not isinstance(v, classmethod)
        ]


class Exporter:
    DUCKDB = "duckdb"
    SQLITE = "sqlite"
    CLICKHOUSE = "clickhouse"

    KAFKA = "kafka"
    KINESIS = "kinesis"
    PUBSUB = "pubsub"
    NATS = "nats"

    GCS = "gcs"
    W3 = "w3"
    MINIO = "minio"

    DATABASE = [DUCKDB, SQLITE, CLICKHOUSE]
    MESSAGE_QUEUE = [KAFKA, NATS, KINESIS, PUBSUB]
    DATALAKE = [GCS, W3, MINIO]

    @classmethod
    def values(cls):
        return [
            v
            for k, v in cls.__dict__.items()
            if not k.startswith("__") and not isinstance(v, classmethod)
        ]
