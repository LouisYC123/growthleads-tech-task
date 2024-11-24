from growthleads_etl import schemas

# Transactional data
WEB_TRAFFIC = {
    "routy": {
        "schema": schemas.RoutyBronzeDataset,
        "archive": False,
        "load_type": "append",
    },
    "voluum": {
        "schema": schemas.VoluumBronzeDataset,
        "archive": False,
        "load_type": "append",
    },
    "manual": {
        "schema": schemas.ManualBronzeDataset,
        "archive": False,
        "load_type": "append",
    },
}
# Slowly changing dimensions
SCD = {
    "deals": {
        "schema": schemas.DealsBronzeDataset,
        "archive": False,
        "load_type": "replace",
    },
    "voluum_mapper": {
        "schema": schemas.VoluumMapperBronzeDataset,
        "archive": False,
        "load_type": "replace",
    },
    "central_mapping": {
        "schema": schemas.CentralMappingBronzeDataset,
        "archive": False,
        "load_type": "replace",
    },
}
