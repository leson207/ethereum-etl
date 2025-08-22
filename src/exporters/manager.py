from src.utils.enumeration import ExporterType


# TODO: re-design here
class ExporterManager:
    def __init__(self, entity_exporter_mapping):
        self.data = {}
        self.entity_exporter_mapping = entity_exporter_mapping
        for entity_type in self.entity_exporter_mapping.data:
            self.data[entity_type] = []

    def get_item(self, entity_type):
        return self.data[entity_type]

    def add_item(self, entity_type, items: list[dict]):
        self.data[entity_type].extend(items)

    def add_exporter(self, entity_type, exporter_type, exporter):
        self.entity_exporter_mapping[entity_type][exporter_type] = exporter

    def export_all(self):
        for entity_type in self.data:
            self.export(entity_type)

    def export(self, entity_type: str):
        for exporter_type, exporter in self.entity_exporter_mapping[
            entity_type
        ].items():
            if exporter is None:
                continue

            if exporter_type in ExporterType.DATABASE:
                exporter.insert(self.data[entity_type], deduplicate="replace")
            else:
                exporter.export(self.data[entity_type])

    def clear_all(self):
        for entity_type in self.data:
            self.clear(entity_type)

    def clear(self, entity_type):
        self.data[entity_type].clear()

        # if len(self.data[entity_type]) > threshold:
        #     for exporter in exporter[entity_type]:
        #         exporter.export(self.data[entity_type])
