from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StandardScaler, StringIndexer, MinMaxScaler, VectorAssembler
from pyspark.ml import Pipeline

project = 'TestProject'
dataset = 'spotify1'
table = 'audio_features'
output_table = 'preprocessed_features'

spark = SparkSession.builder\
    .appName("PreprocessingPipeline")\
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest.jar")\
    .getOrCreate()

df = spark.read.format("bigquery").option("table", table).load()

indexers = [StringIndexer(inputCol=column, outputCol=f"{column}_indexed", handleInvalid="keep")
            for column in ['year', 'genre']]
# Normalization
features = ['popularity','key','duration_ms','loudness','tempo']
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Standardization
features = [
    'popularity',
    'danceability',
    'energy',
    'loudness',
    'speechiness',
    'acousticness',
    'instrumentalness',
    'liveness',
    'valence',
    'tempo',
    'duration_ms',
]
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

pipeline = Pipeline(stages=indexers + [assembler, scaler])
model = pipeline.fit(df)
transformed_df = model.transform(df)
transformed_df.show()
output_table = f'{project}:{dataset}.{output_table}'
transformed_df.write.format('bigquery').option('table', output_table).mode('overwrite').save()