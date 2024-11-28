from growthleads_etl import schemas

# Event data
EVENTS = {
    "routy": {
        "schema": schemas.RoutyBronzeDataset,
        "archive": True,
        "load_type": "append",
        "if_missing": "fail",
    },
    "voluum": {
        "schema": schemas.VoluumBronzeDataset,
        "archive": True,
        "load_type": "append",
        "if_missing": "fail",
    },
    "manual": {
        "schema": schemas.ManualBronzeDataset,
        "archive": True,
        "load_type": "append",
        "if_missing": "skip",
    },
    "scrapers": {
        "schema": schemas.ScrapersBronzeDataset,
        "archive": True,
        "load_type": "append",
        "if_missing": "fail",
    },
}

# Slowly changing dimensions
SCD = {
    "deals": {
        "schema": schemas.DealsBronzeDataset,
        "archive": False,
        "load_type": "replace",
        "if_missing": "fail",
    },
    "voluum_mapper": {
        "schema": schemas.VoluumMapperBronzeDataset,
        "archive": False,
        "load_type": "replace",
        "if_missing": "fail",
    },
    "central_mapping": {
        "schema": schemas.CentralMappingBronzeDataset,
        "archive": False,
        "load_type": "replace",
        "if_missing": "fail",
    },
}
