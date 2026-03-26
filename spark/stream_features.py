from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    count,
    avg,
    stddev,
    max as spark_max,
    min as spark_min,
    when,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
import psycopg2


schema = StructType([
    StructField("tx_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("tx_time", StringType(), True),
    StructField("fraud_label", IntegerType(), True)
])


def write_to_postgres(batch_df, batch_id):
    print(f"\n========== Processing batch {batch_id} ==========")

    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} is empty")
        return

    batch_df.show(50, truncate=False)
    rows = batch_df.collect()

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="bankingdb",
        user="frauduser",
        password="fraudpass"
    )
    cur = conn.cursor()

    for row in rows:
        cur.execute("""
            INSERT INTO fraud.feature_store (
                card_id,
                window_start,
                window_end,
                txn_count_10min,
                avg_amount_10min,
                stddev_amount_10min,
                max_amount_10min,
                min_amount_10min,
                amount_deviation_flag,
                batch_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (card_id, window_start, window_end)
            DO UPDATE SET
                txn_count_10min = EXCLUDED.txn_count_10min,
                avg_amount_10min = EXCLUDED.avg_amount_10min,
                stddev_amount_10min = EXCLUDED.stddev_amount_10min,
                max_amount_10min = EXCLUDED.max_amount_10min,
                min_amount_10min = EXCLUDED.min_amount_10min,
                amount_deviation_flag = EXCLUDED.amount_deviation_flag,
                batch_id = EXCLUDED.batch_id
        """, (
            row["card_id"],
            row["window_start"],
            row["window_end"],
            row["txn_count_10min"],
            row["avg_amount_10min"],
            row["stddev_amount_10min"],
            row["max_amount_10min"],
            row["min_amount_10min"],
            row["amount_deviation_flag"],
            batch_id
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Batch {batch_id}: wrote {len(rows)} rows successfully")


spark = SparkSession.builder \
    .appName("fraud-feature-stream-windowed") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw.card.transactions") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("tx_time", to_timestamp("tx_time"))

features_df = parsed_df.groupBy(
    col("card_id"),
    window(col("tx_time"), "1 minute")
).agg(
    count("*").alias("txn_count_10min"),
    avg("amount").alias("avg_amount_10min"),
    stddev("amount").alias("stddev_amount_10min"),
    spark_max("amount").alias("max_amount_10min"),
    spark_min("amount").alias("min_amount_10min")
).withColumn(
    "amount_deviation_flag",
    when(
        col("stddev_amount_10min").isNotNull() &
        (col("stddev_amount_10min") > 0) &
        (col("max_amount_10min") > col("avg_amount_10min") + 2 * col("stddev_amount_10min")),
        True
    ).otherwise(False)
).select(
    "card_id",
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "txn_count_10min",
    "avg_amount_10min",
    "stddev_amount_10min",
    "max_amount_10min",
    "min_amount_10min",
    "amount_deviation_flag"
)

query = features_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/opt/spark-apps/checkpoints") \
    .start()

query.awaitTermination()