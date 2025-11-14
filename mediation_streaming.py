from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, when, trim, lower
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("Cleaned CDR Mediation Pipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schéma des enregistrements
    schema = StructType() \
    .add("record_type", StringType()) \
    .add("timestamp", StringType()) \
    .add("caller_id", StringType()) \
    .add("callee_id", StringType()) \
    .add("duration_sec", IntegerType()) \
    .add("sender_id", StringType()) \
    .add("receiver_id", StringType()) \
    .add("user_id", StringType()) \
    .add("data_volume_mb", DoubleType()) \
    .add("session_duration_sec", IntegerType()) \
    .add("cell_id", StringType()) \
    .add("technology", StringType())  

    # Lecture depuis Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "records") \
        .option("startingOffsets", "latest") \
        .load()

    # Convertir la valeur Kafka en chaîne
    df_values = df_raw.selectExpr("CAST(value AS STRING) AS json_str")

    # Parsing JSON
    df_parsed = df_values.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    # Nettoyage de base : trim + lowercase pour les IDs
    df_cleaned = df_parsed \
        .withColumn("caller_id", lower(trim(col("caller_id")))) \
        .withColumn("callee_id", lower(trim(col("callee_id")))) \
        .withColumn("sender_id", lower(trim(col("sender_id")))) \
        .withColumn("receiver_id", lower(trim(col("receiver_id")))) \
        .withColumn("user_id", lower(trim(col("user_id")))) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Marquer les lignes comme valides/invalides
    df_validated = df_cleaned.withColumn(
        "is_valid",
        when(
            (col("record_type") == "voice") &
            col("caller_id").isNotNull() &
            col("callee_id").isNotNull() &
            col("duration_sec").isNotNull() &
            (col("duration_sec") > 0) &
            (~col("caller_id").contains("corrupted_data")) &
            (~col("callee_id").contains("corrupted_data")),
            True
        ).when(
            (col("record_type") == "sms") &
            col("sender_id").isNotNull() &
            col("receiver_id").isNotNull() &
            (~col("sender_id").contains("corrupted_data")) &
            (~col("receiver_id").contains("corrupted_data")),
            True
        ).when(
            (col("record_type") == "data") &
            col("user_id").isNotNull() &
            col("data_volume_mb").isNotNull() &
            col("session_duration_sec").isNotNull() &
            (col("data_volume_mb") > 0) &
            (col("session_duration_sec") > 0) &
            (~col("user_id").contains("corrupted_data")),
            True
        ).otherwise(False)
    )

    # Préparer les messages Kafka
    df_valid = df_validated.filter(col("is_valid") == True) \
        .withColumn("value", to_json(struct([col(c) for c in df_validated.columns])))

    df_invalid = df_validated.filter(col("is_valid") == False) \
        .withColumn("value", to_json(struct([col(c) for c in df_validated.columns])))

    # Écriture vers clean_records
    query_valid = df_valid.select("value").writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "clean_records") \
        .option("checkpointLocation", "file:///tmp/checkpoints/clean") \
        .outputMode("append") \
        .start()

    # Écriture vers dirty_records
    query_invalid = df_invalid.select("value").writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "dirty_records") \
        .option("checkpointLocation", "file:///tmp/checkpoints/dirty") \
        .outputMode("append") \
        .start()

    query_valid.awaitTermination()
    query_invalid.awaitTermination()

if __name__ == "__main__":
    main()