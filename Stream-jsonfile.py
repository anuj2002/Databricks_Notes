"""
Complete End-to-End Databricks Spark Streaming to Delta Lake Pipeline
Bhai, yeh Databricks ke liye optimized production-ready code hai!
"""

# ============================================
# STEP 1: Import Libraries (Databricks mein yeh pre-installed hote hain)
# ============================================
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# ============================================
# STEP 2: Configuration Variables
# ============================================
# Databricks paths (DBFS format)
INPUT_PATH = "/mnt/raw-data/orders/"  # Source JSON files path
CHECKPOINT_PATH = "/mnt/checkpoints/orders_stream"  # Checkpoint location
DELTA_TABLE_PATH = "/mnt/delta/orders_fact"  # Delta table location
CATALOG_NAME = "your_catalog"  # Unity Catalog name (optional)
SCHEMA_NAME = "sales"  # Schema/Database name
TABLE_NAME = "orders_fact"  # Table name


# ============================================
# STEP 3: Define Schema (Bahut Important!)
# ============================================
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("customer", StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True)
        ]), True)
    ]), True),
    StructField("items", ArrayType(StructType([
        StructField("item_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True)
    ])), True),
    StructField("payment", StructType([
        StructField("method", StringType(), True),
        StructField("transaction_id", StringType(), True)
    ]), True),
    StructField("metadata", ArrayType(StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True)
    ])), True)
])


# ============================================
# STEP 4: Read Streaming Data
# ============================================
raw_stream = (spark
    .readStream
    .format("json")
    .schema(order_schema)
    .option("maxFilesPerTrigger", 10)  # Batch size control
    .option("recursiveFileLookup", "true")  # Subfolders bhi scan kare
    .load(INPUT_PATH)
)

print("✅ Stream started successfully!")


# ============================================
# STEP 5: Data Transformation & Flattening
# ============================================
transformed_stream = (raw_stream
    # Timestamp conversion
    .withColumn("order_timestamp", to_timestamp(col("timestamp")))
    .withColumn("order_date", to_date(col("timestamp")))
    
    # Customer details flatten
    .withColumn("customer_id", col("customer.customer_id"))
    .withColumn("customer_name", col("customer.name"))
    .withColumn("customer_email", col("customer.email"))
    .withColumn("city", col("customer.address.city"))
    .withColumn("postal_code", col("customer.address.postal_code"))
    .withColumn("country", col("customer.address.country"))
    
    # Payment details
    .withColumn("payment_method", col("payment.method"))
    .withColumn("transaction_id", col("payment.transaction_id"))
    
    # Items array ko explode karo
    .withColumn("item", explode(col("items")))
    .withColumn("item_id", col("item.item_id"))
    .withColumn("product_name", col("item.product_name"))
    .withColumn("quantity", col("item.quantity"))
    .withColumn("price", col("item.price"))
    .withColumn("line_total", col("quantity") * col("price"))
    
    # Metadata processing
    .withColumn("metadata_map", map_from_arrays(col("metadata.key"), col("metadata.value")))
    .withColumn("campaign", col("metadata_map")["campaign"])
    .withColumn("channel", col("metadata_map")["channel"])
    
    # Audit columns
    .withColumn("processed_at", current_timestamp())
    .withColumn("processing_date", current_date())
    .withColumn("file_name", input_file_name())
    
    # Final columns select
    .select(
        "order_id",
        "order_timestamp",
        "order_date",
        "customer_id",
        "customer_name",
        "customer_email",
        "city",
        "postal_code",
        "country",
        "item_id",
        "product_name",
        "quantity",
        "price",
        "line_total",
        "payment_method",
        "transaction_id",
        "campaign",
        "channel",
        "processed_at",
        "processing_date",
        "file_name"
    )
)

print("✅ Transformations applied!")


# ============================================
# STEP 6: Write to Delta Lake (Append Mode)
# ============================================
query_append = (transformed_stream
    .writeStream
    .format("delta")
    .outputMode("append")  # Ya "update" for upserts
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")  # Schema evolution allow kare
    .option("optimizeWrite", "true")  # Auto optimize
    .option("autoCompact", "true")  # Small files merge karein
    .partitionBy("processing_date", "country")  # Partitioning strategy
    .trigger(processingTime="30 seconds")  # Micro-batch interval
    .start(DELTA_TABLE_PATH)
)

print(f"✅ Streaming to Delta table started: {DELTA_TABLE_PATH}")
print(f"📊 Stream ID: {query_append.id}")
print(f"🔄 Status: {query_append.status}")


# ============================================
# STEP 7: Create Delta Table (If not exists)
# ============================================
# Unity Catalog ke saath
table_full_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_full_name}
USING DELTA
LOCATION '{DELTA_TABLE_PATH}'
PARTITIONED BY (processing_date, country)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
""")

print(f"✅ Delta table created/verified: {table_full_name}")


# ============================================
# STEP 8: Monitor Stream (Optional but useful!)
# ============================================
def display_stream_metrics():
    """Stream ki health check karo"""
    for stream in spark.streams.active:
        print(f"\n📊 Stream: {stream.name}")
        print(f"   ID: {stream.id}")
        print(f"   Status: {stream.status}")
        print(f"   Recent Progress:")
        print(stream.lastProgress)

# Call karke dekho
display_stream_metrics()


# ============================================
# STEP 9: ALTERNATIVE - Upsert/Merge Pattern
# ============================================
# Agar duplicate orders ko handle karna hai toh foreachBatch use karo

def upsert_to_delta(microBatchDF, batchId):
    """
    Merge logic for handling duplicates
    """
    # Target Delta table
    delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    
    # Merge condition
    delta_table.alias("target").merge(
        microBatchDF.alias("source"),
        "target.order_id = source.order_id AND target.item_id = source.item_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print(f"✅ Batch {batchId} processed with MERGE")

# Upsert streaming query
query_upsert = (transformed_stream
    .writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", f"{CHECKPOINT_PATH}_upsert")
    .trigger(processingTime="1 minute")
    .start()
)


# ============================================
# STEP 10: Delta Lake Optimization Commands
# ============================================
# Yeh periodically chalao (separate job mein)

# OPTIMIZE command - small files ko merge kare
spark.sql(f"""
OPTIMIZE {table_full_name}
ZORDER BY (order_id, customer_id)
""")

# VACUUM command - old files delete kare (7 days se purani)
spark.sql(f"""
VACUUM {table_full_name} RETAIN 168 HOURS
""")

# Table stats update karo
spark.sql(f"""
ANALYZE TABLE {table_full_name} COMPUTE STATISTICS FOR ALL COLUMNS
""")

print("✅ Optimization commands executed!")


# ============================================
# STEP 11: Monitoring Queries
# ============================================
# Stream status check karo
print("\n🔍 Active Streams:")
for s in spark.streams.active:
    print(f"  - {s.name}: {s.status['message']}")

# Stream stop karne ke liye (testing ke liye)
# query_append.stop()

# Table ka data verify karo
display(spark.sql(f"SELECT * FROM {table_full_name} LIMIT 10"))

# Row count
row_count = spark.sql(f"SELECT COUNT(*) as total FROM {table_full_name}").collect()[0]['total']
print(f"\n📈 Total rows in Delta table: {row_count}")


# ============================================
# STEP 12: Error Handling & Data Quality
# ============================================
# Bad records ko alag table mein store karo

bad_records_path = "/mnt/delta/orders_bad_records"

quality_stream = (raw_stream
    .withColumn("is_valid", 
        when(col("order_id").isNotNull() & col("customer.customer_id").isNotNull(), True)
        .otherwise(False)
    )
    .filter(col("is_valid") == False)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}_bad_records")
    .start(bad_records_path)
)

print("✅ Bad records monitoring started!")


# ============================================
# BONUS: Sample Test Data Generator
# ============================================
# Testing ke liye sample JSON files banao

import json
import random
from datetime import datetime

def generate_test_order(order_num):
    """Test JSON data generate karo"""
    return {
        "order_id": f"ORD{1000 + order_num}",
        "timestamp": datetime.now().isoformat() + "Z",
        "customer": {
            "customer_id": 500 + order_num,
            "name": f"Customer {order_num}",
            "email": f"customer{order_num}@example.com",
            "address": {
                "city": random.choice(["Mumbai", "Delhi", "Bangalore", "Toronto"]),
                "postal_code": f"M5H{order_num:03d}",
                "country": random.choice(["India", "Canada", "USA"])
            }
        },
        "items": [
            {
                "item_id": f"I{100 + i}",
                "product_name": f"Product {i}",
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(10, 100), 2)
            }
            for i in range(random.randint(1, 3))
        ],
        "payment": {
            "method": random.choice(["Credit Card", "Debit Card", "UPI"]),
            "transaction_id": f"TXN{7890 + order_num}"
        },
        "metadata": [
            {"key": "campaign", "value": random.choice(["summer_sale", "back_to_school", "diwali_offer"])},
            {"key": "channel", "value": random.choice(["email", "sms", "app"])}
        ]
    }

# Test data generate karo (optional)
# for i in range(5):
#     order_data = generate_test_order(i)
#     dbutils.fs.put(f"{INPUT_PATH}/order_{i}.json", json.dumps(order_data), overwrite=True)

print("\n✅✅✅ Complete pipeline setup done! ✅✅✅")
print(f"📂 Source: {INPUT_PATH}")
print(f"💾 Delta Table: {DELTA_TABLE_PATH}")
print(f"📋 Table Name: {table_full_name}")
print(f"🔄 Checkpoint: {CHECKPOINT_PATH}")