from src.exporters.mapper import EntityExporterMapper
from src.utils.enumeration import EntityType


def get_mapper(entitty_types, exporter_types):
    mapper = EntityExporterMapper()

    for entitty_type in entitty_types:
        for exporter_type in exporter_types:
            mapper.set_exporter(entitty_type, exporter_type)

    return mapper


def resolve_dependency(target_entity_types):
    dependencies = {
        EntityType.RAW_BLOCK: [],
        EntityType.BLOCK: [EntityType.RAW_BLOCK],
        EntityType.TRANSACTION: [EntityType.RAW_BLOCK],
        EntityType.WITHDRAWAL: [EntityType.RAW_BLOCK],

        EntityType.RAW_RECEIPT: [],
        EntityType.RECEIPT: [EntityType.RAW_RECEIPT],
        EntityType.LOG: [EntityType.RAW_RECEIPT],
        EntityType.TRANSFER: [EntityType.LOG],
        EntityType.POOL: [EntityType.EVENT],
        EntityType.TOKEN: [EntityType.POOL],
        EntityType.EVENT: [EntityType.LOG, EntityType.BLOCK, EntityType.POOL, EntityType.TOKEN],
        EntityType.ACCOUNT: [EntityType.RECEIPT],
        EntityType.CONTRACT: [EntityType.RECEIPT],
        EntityType.ABI: [EntityType.CONTRACT],
        
        EntityType.RAW_TRACE: [],
        EntityType.TRACE: [EntityType.RAW_TRACE],
    }

    require_entity_types = [i for i in target_entity_types]
    for i in require_entity_types:
        for j in dependencies[i]:
            if j not in require_entity_types:
                require_entity_types.append(j)

    return require_entity_types
