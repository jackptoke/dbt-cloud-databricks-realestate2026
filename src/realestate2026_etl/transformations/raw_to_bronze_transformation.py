from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

LANDING = spark.conf.get("landing.path")
CHANNELS = ["buy_properties", "rent_properties", "sold_properties"]


def define_bronze(folder: str) -> None:
    @dp.table(
        name=folder,
        comment=f"Raw listings landed from {LANDING}/{folder}",
        table_properties={
            "quality": "bronze",
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
        },
    )
    @dp.expect("listing_id_present", "listingId IS NOT NULL")
    def _table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .load(f"{LANDING}/{folder}")
            .select(
                "*",
                col("_metadata.file_path").alias("_source_file"),
                current_timestamp().alias("_ingested_at"),
            )
        )


for channel in CHANNELS:
    define_bronze(channel)
