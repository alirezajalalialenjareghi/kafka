
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    lit,
    when,
    to_json,
    struct,
    concat_ws,
    trim
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)


# 1. تعریف Schema
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", LongType(), True),
    StructField("event_time", StringType(), True)
])


def main():

    # 2. ایجاد Spark Session
    spark = SparkSession.builder \
        .appName("KafkaToPostgresStreaming") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 3. خواندن پیام‌ها از Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "orders.events") \
        .option("startingOffsets", "earliest") \
        .load()

    # 4. تبدیل پیام Kafka به JSON
    raw_df = df.select(
        col("value").cast("string").alias("raw_json")
    )

    parsed_df = raw_df.select(
        col("raw_json"),
        from_json(col("raw_json"), schema).alias("data")
    )

    # 5. استخراج فیلدها
    data_df = parsed_df.select(
        col("raw_json"),
        col("data.*")
    )

    # 6. تبدیل تاریخ به Timestamp
    data_df = data_df.withColumn(
        "parsed_event_time",
        to_timestamp(col("event_time"))
    )

    # 7. اعتبارسنجی اطلاعات
    valid_condition = (
        col("event_id").isNotNull() &
        (trim(col("event_id")) != "") &
        col("order_id").isNotNull() &
        (trim(col("order_id")) != "") &
        col("quantity").isNotNull() &
        (col("quantity") > 0) &
        col("unit_price").isNotNull() &
        (col("unit_price") > 0) &
        col("parsed_event_time").isNotNull()
    )

    # 8. داده‌های معتبر
    valid_df = data_df.filter(valid_condition) \
        .withColumn(
            "total_amount",
            col("quantity").cast("long") * col("unit_price")
        ) \
        .withColumn(
            "event_time",
            col("parsed_event_time")
        ) \
        .withColumn(
            "ingested_at",
            current_timestamp()
        ) \
        .select(
            "event_id",
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
            "event_time",
            "ingested_at"
        )

    # 9. داده‌های نامعتبر
    invalid_df = data_df.filter(~valid_condition) \
        .withColumn(
            "error_reason",
            when(
                col("event_id").isNull() |
                (trim(col("event_id")) == ""),
                lit("Invalid or missing event_id")
            ).when(
                col("order_id").isNull() |
                (trim(col("order_id")) == ""),
                lit("Invalid or missing order_id")
            ).when(
                col("quantity").isNull() |
                (col("quantity") <= 0),
                lit("Invalid quantity")
            ).when(
                col("unit_price").isNull() |
                (col("unit_price") <= 0),
                lit("Invalid unit_price")
            ).when(
                col("parsed_event_time").isNull(),
                lit("Invalid event_time")
            ).otherwise(
                lit("Invalid JSON or data")
            )
        ) \
        .withColumn(
            "raw_json",
            col("raw_json")
        )

    # 10. ارسال داده‌های معتبر به PostgreSQL
    def write_to_postgres(batch_df, batch_id):

        if batch_df.isEmpty():
            return

        batch_df.write \
            .format("jdbc") \
            .option(
                "url",
                "jdbc:postgresql://localhost:5432/orders_db"
            ) \
            .option("dbtable", "orders") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    postgres_query = valid_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option(
            "checkpointLocation",
            "/tmp/checkpoint/orders"
        ) \
        .start()

    # 11. ارسال داده‌های نامعتبر به Kafka DLQ
    dlq_df = invalid_df.select(
        to_json(
            struct(
                col("raw_json"),
                col("error_reason")
            )
        ).alias("value")
    )

    dlq_query = dlq_df.writeStream \
        .format("kafka") \
        .option(
            "kafka.bootstrap.servers",
            "localhost:9092"
        ) \
        .option(
            "topic",
            "orders.dlq"
        ) \
        .option(
            "checkpointLocation",
            "/tmp/checkpoint/dlq"
        ) \
        .start()

    print("Spark Kafka Consumer started successfully.")
    print("Valid events -> PostgreSQL")
    print("Invalid events -> Kafka DLQ")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
