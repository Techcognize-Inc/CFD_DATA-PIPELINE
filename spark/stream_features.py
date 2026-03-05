from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg, stddev, max as mx, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Docker-internal endpoints
KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "raw.card.transactions"

PG_JDBC_URL = "jdbc:postgresql://postgres:5432/bankingdb"
PG_USER = "banking"
PG_PASSWORD = "password"
PG_DRIVER = "org.postgresql.Driver"

STAGE_TABLE = "fraud.feature_store_stage"

schema = StructType([
    StructField("tx_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("tx_time", StringType(), True),
    StructField("fraud_label", IntegerType(), True),
])

def call_upsert_function(spark: SparkSession):
    """
    Calls: SELECT fraud.upsert_feature_store();
    using JDBC from the driver (no docker CLI, no psycopg2).
    """
    jvm = spark._sc._gateway.jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", PG_USER)
    props.setProperty("password", PG_PASSWORD)

    conn = jvm.java.sql.DriverManager.getConnection(PG_JDBC_URL, props)
    try:
        stmt = conn.createStatement()
        stmt.execute("SELECT fraud.upsert_feature_store();")
        stmt.close()
    finally:
        conn.close()

def write_batch(batch_df, batch_id: int, spark: SparkSession):
    # 1) write micro-batch to staging table
    (batch_df.write
        .mode("append")
        .format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", STAGE_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", PG_DRIVER)
        .save()
    )
    # 2) merge stage -> target using Postgres function
    call_upsert_function(spark)

def main():
    spark = (SparkSession.builder
             .appName("de2-fraud-stream-features")
             .master("spark://spark-master:7077")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.selectExpr("CAST(value AS STRING) AS json_str")
              .select(from_json(col("json_str"), schema).alias("e"))
              .select(
                  col("e.card_id").alias("card_id"),
                  col("e.amount").cast("double").alias("amount"),
                  col("e.tx_time").cast("timestamp").alias("event_time"),
              )
              .where(col("card_id").isNotNull() & col("amount").isNotNull() & col("event_time").isNotNull())
              .withWatermark("event_time", "5 minutes"))

    features = (parsed
        .groupBy(col("card_id"), window(col("event_time"), "10 minutes", "1 minute").alias("w"))
        .agg(
            count("*").alias("txn_count_10min"),
            avg("amount").alias("avg_amount_10min"),
            stddev("amount").alias("std_amount_10min"),
            mx("amount").alias("max_amount_10min"),
        )
        .select(
            col("card_id"),
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            col("txn_count_10min").cast("int"),
            col("avg_amount_10min"),
            when(
                (col("std_amount_10min").isNotNull()) & (col("std_amount_10min") > 0),
                col("max_amount_10min") > (col("avg_amount_10min") + 3 * col("std_amount_10min"))
            ).otherwise(False).alias("amount_deviation_flag")
        )
    )

    def foreach_batch(df, batch_id):
        write_batch(df, batch_id, spark)

    q = (features.writeStream
         .outputMode("update")
         .foreachBatch(foreach_batch)
         .option("checkpointLocation", "/opt/spark-apps/checkpoints/de2_features")
         .start())

    q.awaitTermination()

if __name__ == "__main__":
    main()