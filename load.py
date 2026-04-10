import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark() -> SparkSession:
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
    s3_endpoint = os.getenv("ICEBERG_S3_ENDPOINT", "http://localhost:9000")
    s3_region = os.getenv("ICEBERG_S3_REGION", "us-east-1")
    s3_access_key = os.getenv("ICEBERG_S3_ACCESS_KEY", "admin")
    s3_secret_key = os.getenv("ICEBERG_S3_SECRET_KEY", "password")

    return (
        SparkSession.builder.appName("iceberg-csv-loader")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", rest_uri)
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse")
        .config(
            "spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.iceberg.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", s3_access_key)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", s3_secret_key)
        .config("spark.sql.catalog.iceberg.s3.region", s3_region)
        .config("spark.sql.catalog.iceberg.client.region", s3_region)
        .getOrCreate()
    )


def read_csv_with_schema(spark: SparkSession, path: str, schema: T.StructType):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(schema)
        .load(path)
    )


def run_sql_file(spark: SparkSession, file_path: Path) -> None:
    sql_text = file_path.read_text(encoding="utf-8").strip()
    spark.sql(sql_text)


def main() -> None:
    spark = build_spark()
    data_dir = Path(__file__).resolve().parent / "data"
    sql_dir = Path(__file__).resolve().parent / "sql" / "load"

    run_sql_file(spark, sql_dir / "01_create_namespace.sql")

    customers_schema = T.StructType(
        [
            T.StructField("customer_id", T.LongType(), False),
            T.StructField("customer_name", T.StringType(), True),
            T.StructField("signup_date", T.DateType(), True),
            T.StructField("region", T.StringType(), True),
        ]
    )

    products_schema = T.StructType(
        [
            T.StructField("product_id", T.LongType(), False),
            T.StructField("product_name", T.StringType(), True),
            T.StructField("category", T.StringType(), True),
            T.StructField("price", T.DecimalType(10, 2), True),
            T.StructField("updated_at", T.TimestampType(), True),
        ]
    )

    orders_schema = T.StructType(
        [
            T.StructField("order_id", T.LongType(), False),
            T.StructField("customer_id", T.LongType(), False),
            T.StructField("order_ts", T.TimestampType(), True),
            T.StructField("order_date", T.DateType(), True),
            T.StructField("status", T.StringType(), True),
            T.StructField("total_amount", T.DecimalType(12, 2), True),
        ]
    )

    order_items_schema = T.StructType(
        [
            T.StructField("order_item_id", T.LongType(), False),
            T.StructField("order_id", T.LongType(), False),
            T.StructField("product_id", T.LongType(), False),
            T.StructField("order_date", T.DateType(), True),
            T.StructField("qty", T.IntegerType(), True),
            T.StructField("unit_price", T.DecimalType(10, 2), True),
        ]
    )

    events_schema = T.StructType(
        [
            T.StructField("event_id", T.LongType(), False),
            T.StructField("customer_id", T.LongType(), False),
            T.StructField("product_id", T.LongType(), True),
            T.StructField("order_id", T.LongType(), True),
            T.StructField("session_id", T.StringType(), True),
            T.StructField("event_type", T.StringType(), False),
            T.StructField("event_ts", T.TimestampType(), True),
            T.StructField("event_date", T.DateType(), True),
            T.StructField("event_year", T.IntegerType(), False),
            T.StructField("event_month", T.IntegerType(), False),
            T.StructField("event_day", T.IntegerType(), False),
            T.StructField("quantity", T.IntegerType(), True),
            T.StructField("amount", T.DecimalType(12, 2), True),
        ]
    )

    customers_df = read_csv_with_schema(
        spark, str(data_dir / "customers.csv"), customers_schema
    )
    products_df = read_csv_with_schema(
        spark, str(data_dir / "products.csv"), products_schema
    )
    orders_df = read_csv_with_schema(spark, str(data_dir / "orders.csv"), orders_schema)
    order_items_df = read_csv_with_schema(
        spark, str(data_dir / "order_items.csv"), order_items_schema
    )
    events_df = read_csv_with_schema(spark, str(data_dir / "events.csv"), events_schema)

    run_sql_file(spark, sql_dir / "02_drop_customers.sql")
    run_sql_file(spark, sql_dir / "03_drop_products.sql")
    run_sql_file(spark, sql_dir / "04_drop_orders.sql")
    run_sql_file(spark, sql_dir / "05_drop_order_items.sql")
    run_sql_file(spark, sql_dir / "06_drop_events.sql")
    run_sql_file(spark, sql_dir / "07_create_customers.sql")
    run_sql_file(spark, sql_dir / "08_create_products.sql")
    run_sql_file(spark, sql_dir / "09_create_orders.sql")
    run_sql_file(spark, sql_dir / "10_create_order_items.sql")
    run_sql_file(spark, sql_dir / "11_create_events.sql")

    customers_df.sortWithinPartitions("customer_id", "signup_date").writeTo(
        "iceberg.sales.customers"
    ).append()
    products_df.sortWithinPartitions("product_id").writeTo(
        "iceberg.sales.products"
    ).append()
    orders_df.repartition("order_date", "customer_id").sortWithinPartitions(
        "order_date", "customer_id", "order_id"
    ).writeTo("iceberg.sales.orders").append()
    order_items_df.repartition("order_date", "order_id").sortWithinPartitions(
        "order_date", "order_id", "product_id"
    ).writeTo("iceberg.sales.order_items").append()
    events_df.repartition(
        "event_year", "event_month", "event_day"
    ).sortWithinPartitions(
        "event_year", "event_month", "event_day", "customer_id", "event_ts"
    ).writeTo("iceberg.sales.events").append()

    print("Loaded rows:")
    print("customers  ->", spark.table("iceberg.sales.customers").count())
    print("products   ->", spark.table("iceberg.sales.products").count())
    print("orders     ->", spark.table("iceberg.sales.orders").count())
    print("order_items->", spark.table("iceberg.sales.order_items").count())
    print("events     ->", spark.table("iceberg.sales.events").count())

    spark.stop()


if __name__ == "__main__":
    main()
