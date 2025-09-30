# stream-processor/processor.py
"""
PySpark Structured Streaming processor
Processes real-time data from Kafka and writes to PostgreSQL
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamProcessor:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'warehouse'),
            'user': os.getenv('POSTGRES_USER', 'admin'),
            'password': os.getenv('POSTGRES_PASSWORD', 'admin123')
        }
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("RetailStreamProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                    "org.postgresql:postgresql:42.5.1") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schemas
        self.transaction_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("customer_type", StringType(), True),
            StructField("items", ArrayType(
                StructType([
                    StructField("product_id", StringType()),
                    StructField("product_name", StringType()),
                    StructField("category", StringType()),
                    StructField("quantity", IntegerType()),
                    StructField("unit_price", DoubleType()),
                    StructField("total", DoubleType())
                ])
            )),
            StructField("total_amount", DoubleType(), False),
            StructField("payment_method", StringType(), True),
            StructField("discount_applied", DoubleType(), True),
            StructField("final_amount", DoubleType(), False)
        ])
        
    def create_postgres_tables(self):
        """Create necessary tables in PostgreSQL if they don't exist"""
        conn = psycopg2.connect(**self.postgres_config)
        cur = conn.cursor()
        
        # Transactions summary table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions_summary (
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                store_id VARCHAR(50),
                total_transactions BIGINT,
                total_revenue DECIMAL(10, 2),
                avg_transaction_value DECIMAL(10, 2),
                unique_customers BIGINT,
                PRIMARY KEY (window_start, store_id)
            )
        """)
        
        # Real-time metrics table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS realtime_metrics (
                metric_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metric_name VARCHAR(50),
                metric_value DECIMAL(10, 2),
                dimension VARCHAR(50)
            )
        """)
        
        # Product performance table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_performance (
                window_start TIMESTAMP,
                product_id VARCHAR(20),
                product_name VARCHAR(100),
                category VARCHAR(50),
                quantity_sold BIGINT,
                revenue DECIMAL(10, 2),
                PRIMARY KEY (window_start, product_id)
            )
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        logger.info("PostgreSQL tables created successfully")
    
    def process_transactions_stream(self):
        """Process transaction stream from Kafka"""
        
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "retail-transactions") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON and apply schema
        transactions_df = df.select(
            from_json(col("value").cast("string"), self.transaction_schema).alias("data")
        ).select("data.*")
        
        # Add processing timestamp and watermark
        transactions_df = transactions_df \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("event_time", to_timestamp(col("timestamp"))) \
            .withWatermark("event_time", "1 minute")
        
        # Aggregate by store with windowing
        # FIXED: Using approx_count_distinct instead of countDistinct for streaming
        store_aggregates = transactions_df \
            .groupBy(
                window(col("event_time"), "1 minute", "30 seconds"),
                col("store_id")
            ) \
            .agg(
                count("transaction_id").alias("total_transactions"),
                sum("final_amount").alias("total_revenue"),
                avg("final_amount").alias("avg_transaction_value"),
                approx_count_distinct("customer_id").alias("unique_customers")  # FIXED: Changed from countDistinct
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("store_id"),
                col("total_transactions"),
                col("total_revenue"),
                col("avg_transaction_value"),
                col("unique_customers")
            )
        
        # Write aggregated data to PostgreSQL
        query = store_aggregates \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(lambda df, epoch_id: self.write_to_postgres(df, "transactions_summary")) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        return query
    
    def process_product_performance(self):
        """Process product performance metrics"""
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "retail-transactions") \
            .option("startingOffsets", "latest") \
            .load()
        
        transactions_df = df.select(
            from_json(col("value").cast("string"), self.transaction_schema).alias("data")
        ).select("data.*")
        
        # Explode items array to get individual products
        products_df = transactions_df \
            .withColumn("event_time", to_timestamp(col("timestamp"))) \
            .withWatermark("event_time", "1 minute") \
            .select(
                col("event_time"),
                explode(col("items")).alias("item")
            ) \
            .select(
                col("event_time"),
                col("item.product_id").alias("product_id"),
                col("item.product_name").alias("product_name"),
                col("item.category").alias("category"),
                col("item.quantity").alias("quantity"),
                col("item.total").alias("revenue")
            )
        
        # Aggregate product performance
        product_aggregates = products_df \
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("product_id"),
                col("product_name"),
                col("category")
            ) \
            .agg(
                sum("quantity").alias("quantity_sold"),
                sum("revenue").alias("revenue")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("product_id"),
                col("product_name"),
                col("category"),
                col("quantity_sold"),
                col("revenue")
            )
        
        query = product_aggregates \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(lambda df, epoch_id: self.write_to_postgres(df, "product_performance")) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        return query
    
    def write_to_postgres(self, df, table_name):
        """Write DataFrame to PostgreSQL"""
        try:
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{self.postgres_config['host']}:5432/{self.postgres_config['database']}") \
                .option("dbtable", table_name) \
                .option("user", self.postgres_config['user']) \
                .option("password", self.postgres_config['password']) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            logger.info(f"Written {df.count()} records to {table_name}")
        except Exception as e:
            logger.error(f"Error writing to PostgreSQL: {e}")
    
    def calculate_real_time_metrics(self):
        """Calculate and display real-time metrics"""
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", "retail-transactions") \
            .option("startingOffsets", "latest") \
            .load()
        
        transactions_df = df.select(
            from_json(col("value").cast("string"), self.transaction_schema).alias("data")
        ).select("data.*")
        
        # Calculate streaming metrics
        metrics_df = transactions_df \
            .withColumn("event_time", to_timestamp(col("timestamp"))) \
            .withWatermark("event_time", "1 minute") \
            .groupBy(
                window(col("event_time"), "30 seconds")
            ) \
            .agg(
                count("transaction_id").alias("transactions_per_window"),
                sum("final_amount").alias("revenue_per_window"),
                avg("final_amount").alias("avg_transaction_value")
            )
        
        # Output to console for monitoring
        query = metrics_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        return query
    
    def run(self):
        """Main execution method"""
        logger.info("Starting Stream Processor...")
        
        # Create tables
        self.create_postgres_tables()
        
        # Start all streaming queries
        queries = []
        
        # Process transactions
        logger.info("Starting transactions processing...")
        queries.append(self.process_transactions_stream())
        
        # Process product performance
        logger.info("Starting product performance processing...")
        queries.append(self.process_product_performance())
        
        # Calculate real-time metrics
        logger.info("Starting real-time metrics calculation...")
        queries.append(self.calculate_real_time_metrics())
        
        # Wait for all streams to terminate
        for query in queries:
            query.awaitTermination()

if __name__ == "__main__":
    processor = StreamProcessor()
    processor.run()