from src.exporters.mapper import EntityExporterMapper


def get_mapper(entitty_types, exporter_types):
    mapper = EntityExporterMapper()

    for entitty_type in entitty_types:
        for exporter_type in exporter_types:
            mapper.set_exporter(entitty_type, exporter_type)

    return mapper
