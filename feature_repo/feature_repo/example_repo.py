from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType, FeatureService
from feast.types import Float32, Int64

iris_stats_source = FileSource(
    name = "iris_stats_source",
    path="/home/sbalawajder/projects/train/feast-mlflow/feast-mlflow-project/feature_repo/feature_repo/data/iris_stats.parquet",
    timestamp_field="event_timestamp",
)

iris = Entity(name="iris", join_keys=["iris_id"])

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
iris_stats_fv = FeatureView(
    name="iris_stats_fv",
    entities=[iris],  # reference entity by name
    ttl=timedelta(days=1),
    schema=[
        Field(name="sepal_length_cm", dtype=Float32),
        Field(name="sepal_width_cm", dtype=Float32),
        Field(name="petal_length_cm", dtype=Float32),
        Field(name="petal_width_cm", dtype=Float32),
        Field(name="target", dtype=Int64),
    ],
    online=True,
    source=iris_stats_source
)

iris_stats_fs = FeatureService(
    name="iris_stats_fs",
    features=[iris_stats_fv]
)

