from src.exporters.mapper import EntityExporterMapper
from src.utils.enumeration import EntityType, ExporterType


class ExportManager:
    def __init__(self, mapper: EntityExporterMapper):
        self.data = {}
        for entity_type in EntityType.values():
            self.data[entity_type] = []

        self.mapper = mapper

    def add_items(self, key, items: list):
        self.data[key].extend(items)

    def __getitem__(self, key):
        return self.data[key]

    def clear_all(self):
        for key in self.data:
            self.clear(key)

    def clear(self, key):
        self.data[key].clear()

    def export_all(self):
        for entity_type in self.data:
            self.export(entity_type)

    def exports(self, entity_types: list[str]):
        for entity_type in entity_types:
            self.export(entity_type)

    def export(self, entity_type: str):
        for exporter_type, exporter in self.mapper[entity_type].items():
            if exporter is None:
                continue

            if exporter_type in ExporterType.DATABASE:
                exporter.insert(self.data[entity_type], deduplicate="replace")
            else:
                exporter.export(self.data[entity_type])
