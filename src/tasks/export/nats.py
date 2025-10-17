import json

import orjson

from src.configs.connection_manager import connection_manager
from src.utils.enumeration import Entity
from src.configs.environment import env
from src.logger import logger


def create_export_function(entity):
    async def export(results: dict[str, list], **kwargs):
        for item in results[entity]:
            try:
                payload = orjson.dumps(item)
            except TypeError:
                payload = json.dumps(item).encode()

            await connection_manager["jetstream"].publish(
                subject=f"{env.NETWORK}{env.ENVIRONMENT_NAME}.{entity}", payload=payload
            )  # TypeError: Integer exceeds 64-bit range
            
        logger.info(f"Publish {len(results[entity])} entity to {env.NETWORK}{env.ENVIRONMENT_NAME}.{entity} subject!")

    export.__name__ = f"export_{entity}"
    return export


entity_func = {ent: create_export_function(ent) for ent in Entity.values()}


# entity_func = {
#     Entity.RAW_BLOCK: create_export_function(Entity.RAW_BLOCK),
#     Entity.BLOCK: create_export_function(Entity.BLOCK),
#     Entity.TRANSACTION: create_export_function(Entity.TRANSACTION),
#     Entity.WITHDRAWAL: create_export_function(Entity.WITHDRAWAL),
#     Entity.RAW_RECEIPT: create_export_function(Entity.RAW_RECEIPT),
#     Entity.RECEIPT: create_export_function(Entity.RECEIPT),
#     Entity.LOG: create_export_function(Entity.LOG),
#     Entity.TRANSFER: create_export_function(Entity.TRANSFER),
#     Entity.EVENT: create_export_function(Entity.EVENT),
#     Entity.ACCOUNT: create_export_function(Entity.ACCOUNT),
#     Entity.POOL: create_export_function(Entity.POOL),
#     Entity.TOKEN: create_export_function(Entity.TOKEN),
#     Entity.RAW_TRACE: create_export_function(Entity.RAW_TRACE),
#     Entity.TRACE: create_export_function(Entity.TRACE),
# }
