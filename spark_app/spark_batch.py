from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("iot-batch-analysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv("/opt/spark-data/iot_data_large.csv", header=True, inferSchema=True)

df_clean = df.withColumn("temperature", col("temperature").cast("double")).withColumn("humidity", col("humidity").cast("double")).withColumn("timestamp", col("timestamp").cast("timestamp"))

agg = df_clean.groupBy("city").agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
)

agg.show()

agg.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/demo_iot").option("dbtable", "iot_batch_metrics").option("user", "demo").option("password", "demo").option("driver", "org.postgresql.Driver").mode("overwrite").save()