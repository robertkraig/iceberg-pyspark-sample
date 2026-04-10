import os
from pathlib import Path

from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
    s3_endpoint = os.getenv("ICEBERG_S3_ENDPOINT", "http://localhost:9000")
    s3_region = os.getenv("ICEBERG_S3_REGION", "us-east-1")
    s3_access_key = os.getenv("ICEBERG_S3_ACCESS_KEY", "admin")
    s3_secret_key = os.getenv("ICEBERG_S3_SECRET_KEY", "password")

    return (
        SparkSession.builder.appName("iceberg-query")
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


def read_sql_file(file_path: Path) -> str:
    return file_path.read_text(encoding="utf-8").strip()


def main() -> None:
    spark = build_spark()
    sql_dir = Path(__file__).resolve().parent / "sql" / "query"

    print("Event counts by day and type (year/month/day partitions)")
    spark.sql(read_sql_file(sql_dir / "01_orders_in_january.sql")).show(truncate=False)

    print("January events joined to customers/products")
    spark.sql(read_sql_file(sql_dir / "02_line_totals_by_order.sql")).show(
        truncate=False
    )

    print("Purchase events joined to orders, items, customers, products")
    spark.sql(read_sql_file(sql_dir / "03_joined_order_details.sql")).show(
        truncate=False
    )

    print("Top customers by event revenue")
    spark.sql(read_sql_file(sql_dir / "04_top_customers_by_spend.sql")).show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()
