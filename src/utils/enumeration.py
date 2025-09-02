class EntityType:
    RAW_BLOCK = "raw_block"
    BLOCK = "block"
    TRANSACTION = "transaction"
    WITHDRAWAL = "withdrawal"

    RAW_RECEIPT = "raw_receipt"
    RECEIPT = "receipt"
    LOG = "log"
    EVENT = "event"

    RAW_TRACE = "raw_trace"
    TRACE = "trace"

    @classmethod
    def values(cls):
        return [
            v
            for k, v in cls.__dict__.items()
            if not k.startswith("__") and not isinstance(v, classmethod)
        ]


class ExporterType:
    DUCKDB = "duckdb"
    SQLITE = "sqlite"
    CLICKHOUSE = "clickhouse"

    KAFKA = "kafka"
    KINESIS = "kinesis"
    PUBSUB = "pubsub"

    GCS = "gcs"
    W3 = "w3"
    MINIO = "minio"

    DATABASE = [DUCKDB, SQLITE, CLICKHOUSE]
    MESSAGE_QUEUE = [KAFKA, KINESIS, PUBSUB]
    DATALAKE = [GCS, W3, MINIO]

    @classmethod
    def values(cls):
        return [
            v
            for k, v in cls.__dict__.items()
            if not k.startswith("__") and not isinstance(v, classmethod)
        ]
