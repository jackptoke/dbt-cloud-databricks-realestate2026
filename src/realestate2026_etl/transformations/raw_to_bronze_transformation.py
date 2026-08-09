from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

LANDING = spark.conf.get("landing.path")
CHANNELS = ["buy_properties", "rent_properties", "sold_properties"]

# Auto Loader infers the schema from the records it actually sees, so a field
# that no record in a crawl happens to carry becomes an ABSENT COLUMN rather
# than a null one. Downstream SQL referencing it fails outright:
#
#   [UNRESOLVED_COLUMN] `auctionTime`.`auction` cannot be resolved
#
# That is not hypothetical — only 1 of 674 buy listings in dev had an auction,
# and prod's first 267 had none, so prod's buy table came out with 46 columns
# against dev's 48 and broke the dbt build.
#
# Schema hints pin these columns into existence with a declared type whether or
# not the data contains them. Two benefits: staging models can reference an
# optional field unconditionally, and bronze schemas stay identical across
# environments instead of drifting with each crawl's contents.
#
# Only optional fields need hinting. Anything present on every record (address,
# agency, price, listingId) is inferred consistently and is left alone.

COMMON_HINTS = (
    # Populated for sold listings, largely absent on active ones.
    "status struct<label:string,type:string>, "
    # Always {"value": ""} in practice, but a crawl of only populated records
    # would otherwise infer a different shape.
    "modifiedDate struct<value:string>, "
)

INSPECTIONS = (
    "inspectionsAndAuctions array<struct<auction:boolean,dateDisplay:string,"
    "endTime:string,endTimeDisplay:string,startTime:string,"
    "startTimeDisplay:string>>, "
)

LAND_SIZE = (
    "landSize struct<display:string,displayApp:string,"
    "displayAppAbbreviated:string,unit:string,value:bigint>, "
)

CHANNEL_HINTS = {
    "buy_properties": (
        "auctionTime struct<auction:boolean,dateDisplay:string,"
        "startTime:string,startTimeDisplay:string>, "
        "statementOfInformation struct<href:string,statementSummary:string,"
        "title:string>, "
        "builderProfile struct<hasDesignsOnPage:boolean>, "
        "agencyListingId string, "
        "constructionStatus string, "
        "isExternalChildListing boolean, "
        "isInternalChildListing boolean, "
        "isLinkedExternalChildListing boolean, "
        + LAND_SIZE
        + INSPECTIONS
    ),
    "rent_properties": (
        "bond struct<display:string,value:bigint>, "
        "dateAvailable struct<date:string,dateDisplay:string>, "
        "applyOnline boolean, "
        "agencyListingId string, "
        + INSPECTIONS
    ),
    "sold_properties": (
        "dateSold struct<display:string,value:string>, "
        "constructionStatus string, "
        "propertyTypeDisplay string, "
        "propertyTypeId string, "
        + LAND_SIZE
    ),
}


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
            # Auto Loader remembers files by path and, by default, refuses to
            # look at one twice. The landing writer overwrites a page in place
            # on a replay, so without this a CORRECTED page would sit in the
            # volume and never reach bronze — the silent half of a bug whose
            # loud half (duplicate rows) staging already handles by keeping the
            # latest _ingested_at per (listingId, ingest_date).
            .option("cloudFiles.allowOverwrites", "true")
            # Landed pages are always page=NNNN.jsonl. Anything else in the
            # directory is not data.
            .option("pathGlobFilter", "*.jsonl")
            .option(
                "cloudFiles.schemaHints",
                (COMMON_HINTS + CHANNEL_HINTS[folder]).rstrip(", "),
            )
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
