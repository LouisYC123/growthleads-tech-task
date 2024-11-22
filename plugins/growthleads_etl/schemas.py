from pandera import Column, DataFrameSchema
import pandera as pa


RoutyBronzeDataset = DataFrameSchema(
    {
        "date": Column(pa.DateTime, nullable=True),
        "marketing_source": Column(pa.String, nullable=True),
        "operator": Column(pa.String, nullable=True),
        "country": Column(pa.String, nullable=True),
        "country_code": Column(pa.String, nullable=True),
        "raw_earnings": Column(pa.Float, nullable=True),
        "visits": Column(pa.Float, nullable=True),
        "signups": Column(pa.Float, nullable=True),
        "source": Column(pa.String, nullable=True),
        "filename": Column(pa.String, nullable=True),
        "ingestion_timestamp": Column(pa.Timestamp, nullable=True),
    }
)

VoluumBronzeDataset = DataFrameSchema(
    {
        "date": Column(pa.DateTime, nullable=True),
        "voluum_brand": Column(pa.String, nullable=True),
        "clicks": Column(pa.Float, nullable=True),
        "source": Column(pa.String, nullable=True),
        "filename": Column(pa.String, nullable=True),
        "ingestion_timestamp": Column(pa.Timestamp, nullable=True),
    }
)
