from pandera import Field, DataFrameModel
from functools import partial
import pandas as pd

Coerced = partial(Field, coerce=True)
Unique = partial(Coerced, unique=True)
Nullable = partial(Coerced, nullable=True)


class BaseBronzeSchema(DataFrameModel):
    source: pd.StringDtype = Coerced()
    filename: pd.StringDtype = Coerced()
    ingestion_timestamp: pd.Timestamp = Coerced()
    source_id: pd.StringDtype = Coerced()


class RoutyBronzeDataset(BaseBronzeSchema):
    date: pd.Timestamp = Nullable()
    marketing_source: pd.StringDtype = Nullable()
    operator: pd.StringDtype = Nullable()
    country: pd.StringDtype = Nullable()
    country_code: pd.StringDtype = Nullable()
    raw_earnings: pd.Float64Dtype = Nullable()
    visits: pd.Float64Dtype = Nullable()
    signups: pd.Float64Dtype = Nullable()


class VoluumBronzeDataset(BaseBronzeSchema):
    date: pd.Timestamp = Nullable()
    voluum_brand: pd.StringDtype = Nullable()
    clicks: pd.Float64Dtype = Nullable()


class ManualBronzeDataset(BaseBronzeSchema):
    date: pd.Timestamp = Nullable()
    marketing_source: pd.StringDtype = Nullable()
    operator: pd.StringDtype = Nullable()
    raw_earnings: pd.Float64Dtype = Nullable()
    visits: pd.Float64Dtype = Nullable()
    signups: pd.Float64Dtype = Nullable()


class DealsBronzeDataset(BaseBronzeSchema):
    marketing_source: pd.StringDtype = Nullable()
    deal: pd.StringDtype = Nullable()
    comments: pd.StringDtype = Nullable()


class VoluumMapperBronzeDataset(BaseBronzeSchema):
    voluum_brand: pd.StringDtype = Nullable()
    marketing_source: pd.StringDtype = Nullable()


class CentralMappingBronzeDataset(BaseBronzeSchema):
    variants: pd.StringDtype = Nullable()
    std_operator: pd.StringDtype = Nullable()
    country: pd.StringDtype = Nullable()
