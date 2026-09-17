from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. Read Bronze as a STREAM instead of a static snapshot
bronze_stream = spark.readStream.table("shop_stream.core.orders")

# 2. Same transformation logic as before, just applied to a streaming DataFrame
def transform(df):
    return (
        df
        .withColumn("order_line_id", F.coalesce(F.col("order_line_id"), F.col("event_id")))
        .withColumn(
            "order_timestamp",
            F.coalesce(F.to_timestamp(F.col("order_ts").cast("string")), F.to_timestamp(F.col("event_ts")))
        )
        .withColumn(
            "source_system",
            F.when(F.col("event_id").isNotNull(), F.lit("streaming")).otherwise(F.lit("batch"))
        )
        .withColumn("status", F.initcap(F.col("status")))
        .drop("order_ts", "event_ts", "event_id", "_rescued_data")
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") >= 0)
        .filter(F.col("order_line_id").isNotNull())
        .filter(F.col("order_timestamp").isNotNull())
    )

silver_stream_df = transform(bronze_stream)

# 3. Add columns the streaming transform produces but the batch-written silver table lacks
existing_cols = set(DeltaTable.forName(spark, "shop_stream.silver.orders").toDF().columns)
if "order_timestamp" not in existing_cols:
    spark.sql("ALTER TABLE shop_stream.silver.orders ADD COLUMN order_timestamp TIMESTAMP")
if "source_system" not in existing_cols:
    spark.sql("ALTER TABLE shop_stream.silver.orders ADD COLUMN source_system STRING")

# 4. The merge logic that runs on every micro-batch of new data
def upsert_to_silver(microbatch_df, batch_id):
    # guard against duplicates arriving within the same micro-batch
    microbatch_df = microbatch_df.dropDuplicates(["order_line_id"])

    target = DeltaTable.forName(spark, "shop_stream.silver.orders")
    src_cols = microbatch_df.columns
    (
        target.alias("t")
        .merge(microbatch_df.alias("s"), "t.order_line_id = s.order_line_id")
        .whenMatchedUpdate(set={c: f"s.{c}" for c in src_cols if c != "order_line_id"})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in src_cols})
        .execute()
    )

# 5. Start the stream
query = (
    silver_stream_df.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", "/Volumes/shop_stream/silver/streaming/checkpoints/orders")
    .trigger(availableNow=True)   # process what's currently in Bronze, then stop — matches your Auto Loader style
    .start()
)

query.awaitTermination()
