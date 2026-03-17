from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

PG_JDBC_URL = "jdbc:postgresql://de2-postgres:5432/bankingdb"
PG_USER = "banking"
PG_PASSWORD = "password"
PG_DRIVER = "org.postgresql.Driver"

FEATURE_TABLE = "fraud.feature_store"
ALERT_TABLE = "fraud.fraud_alerts"


def main():
    spark = (
        SparkSession.builder
        .appName("de2-fraud-rule-engine")
        .master("spark://de2-spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    features_df = (
        spark.read
        .format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", FEATURE_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", PG_DRIVER)
        .load()
    )

    alerts_df = (
        features_df
        .filter(
            (col("txn_count_10min") > 5) |
            (col("amount_deviation_flag") == True)
        )
        .withColumn(
            "alert_reason",
            when(
                (col("txn_count_10min") > 5) & (col("amount_deviation_flag") == True),
                lit("High txn count and amount deviation")
            ).when(
                col("txn_count_10min") > 5,
                lit("High txn count")
            ).when(
                col("amount_deviation_flag") == True,
                lit("Amount deviation")
            )
        )
        .select(
            col("card_id"),
            col("window_start"),
            col("window_end"),
            col("alert_reason")
        )
    )

    (
        alerts_df.write
        .mode("append")
        .format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", ALERT_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", PG_DRIVER)
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()