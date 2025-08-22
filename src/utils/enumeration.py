class EntityType:
    RAW_BLOCK = "raw_block"
    BLOCK = "block"
    TRANSACTION = "transaction"
    WITHDRAWAL = "withdrawal"


class ExporterType:
    DUCKDB = "duckdb"
    CLICKHOUSE = "clickhouse"

    KAFKA = "kafka"
    KINESIS = "kinesis"
    PUBSUB = "pubsub"

    GCS = "gcs"
    W3 = "w3"
    MINIO = "minio"

    DATABASE = [DUCKDB, CLICKHOUSE]
    MESSAGE_QUEUE = [KAFKA, KINESIS, PUBSUB]
    DATALAKE = [GCS, W3, MINIO]
