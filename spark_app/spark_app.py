from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("iot-analysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = (StructType()
          .add("timestamp", StringType())
          .add("city", StringType())
          .add("temperature", DoubleType())
          .add("humidity", DoubleType()))

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "iot-sensor")
       .option("startingOffsets", "earliest")
       .load())

data = (raw.selectExpr("CAST(value AS STRING) as json")
             .select(from_json(col("json"), schema).alias("d"))
             .select("d.*")
             .withColumn("timestamp", col("timestamp").cast(TimestampType())))

agg = (data.withWatermark("timestamp", "1 minute")
             .groupBy(window(col("timestamp"), "1 minute"), col("city"))
             .agg(avg("temperature").alias("avg_temperature"),
                  avg("humidity").alias("avg_humidity"))
             .select(col("window.start").alias("window_start"),
                     col("window.end").alias("window_end"),
                     "city", "avg_temperature", "avg_humidity"))

def to_postgres(df, _):
    (df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/demo_iot")
        .option("dbtable", "iot_metrics")
        .option("user", "demo")
        .option("password", "demo")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())

(agg.writeStream
     .outputMode("update")
     .foreachBatch(to_postgres)
     .option("checkpointLocation", "/tmp/checkpoint")
     .start()
     .awaitTermination())