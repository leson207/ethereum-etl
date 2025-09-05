from src.utils.enumeration import EntityType


class DependencyResolver:
    def __init__(self):
        self.dependencies = {
            EntityType.RAW_BLOCK: [],
            EntityType.BLOCK: [EntityType.RAW_BLOCK],
            EntityType.TRANSACTION: [EntityType.RAW_BLOCK],
            EntityType.WITHDRAWAL: [EntityType.RAW_BLOCK],
            EntityType.RAW_RECEIPT: [],
            EntityType.RECEIPT: [EntityType.RAW_RECEIPT],
            EntityType.LOG: [EntityType.RAW_RECEIPT],
            EntityType.RAW_TRACE: [],
            EntityType.TRACE: [EntityType.RAW_TRACE],
        }

    def resolve(self, entity):
        result = set()

        def dfs(node):
            for dep in self.dependencies.get(node, []):
                if dep not in result:
                    result.add(dep)
                    dfs(dep)

        dfs(entity)
        return list(result)