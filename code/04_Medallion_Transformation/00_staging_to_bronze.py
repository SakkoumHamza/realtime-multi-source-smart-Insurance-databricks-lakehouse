import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import MapType, StructType, StructField, StringType, DoubleType, TimestampType

catalog = "claims_catalog"
bronze_schema = "01_bronze"

blob_staging_schema="00_blob-storage-staging"
sql_server_staging_schema = "00_sql-server-staging"
eventhub_staging_schema = "00_event-hub-staging"

# ───────────────────────────────────────────────
# Images (00_blob-storage-staging→ → Bronze)
# ───────────────────────────────────────────────

# ───────── TRAINING IMAGES ─────────

@dlt.table(
    name=f"{catalog}.{bronze_schema}.training_images",
    comment="Raw accident training images ingested from BlobStorage",
    table_properties={"quality": "bronze"}
)
def training_images_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load(f"/Volumes/{catalog}/{blob_staging_schema}/training-images")
)


# ───────── CLAIM IMAGE METADATA ─────────

@dlt.table(
    name=f"{catalog}.{bronze_schema}.claim_images_meta",
    comment="Claim image metadata ingested from Blob storage",
    table_properties={"quality": "bronze"}
)
def claim_images_meta_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(f"/Volumes/{catalog}/{blob_staging_schema}/metadata")
    )


# ───────────────────────────────────────────────
# TELEMATICS (00_event-hub-staging → Bronze)
# ───────────────────────────────────────────────

# JSON from Event Hub is a generic string map


@dlt.table(
    name=f"{catalog}.{bronze_schema}.telematics",
    comment="Bronze parsed telematics data from Event Hub staging",
    table_properties={"quality": "bronze"}
)
def telematics_bronze():

    df = spark.readStream.table("`claims_catalog`.`00_event-hub-staging`.`claims`")

    # value is an escaped JSON string → clean it
    cleaned = df.withColumn("clean_json",
        regexp_replace(col("value").cast("string"), '\\"', '"')
    )

    return cleaned.select(
        get_json_object(col("clean_json"), '$.chassis_no').alias("chassis_no"),
        get_json_object(col("clean_json"), '$.latitude').cast("double").alias("latitude"),
        get_json_object(col("clean_json"), '$.longitude').cast("double").alias("longitude"),
        get_json_object(col("clean_json"), '$.speed').cast("double").alias("speed"),
        get_json_object(col("clean_json"), '$.event_timestamp').alias("event_timestamp"),

        # metadata
        col("timestamp").alias("eventhub_timestamp"),
        col("offset"),
        col("partition"),
        col("_fivetran_synced"),

        # raw payload
        col("clean_json").alias("_raw_event")
    )

# ───────────────────────────────────────────────
# POLICY (00_sql-server-staging→ Bronze)
# ───────────────────────────────────────────────
@dlt.table(
    name=f"{catalog}.{bronze_schema}.policy",
    comment="Raw policy table from SQL Server staging",
    table_properties={"quality": "bronze"}
)
def policy_bronze():
    return spark.readStream.table(
        "`claims_catalog`.`00_sql-server-staging`.`policy`"
    )


# ───────────────────────────────────────────────
# CLAIM (00_sql-server-staging → Bronze)
# ───────────────────────────────────────────────
@dlt.table(
    name=f"{catalog}.{bronze_schema}.claim",
    comment="Raw claim table from SQL Server staging",
    table_properties={"quality": "bronze"}
)
def claim_bronze():
    return spark.readStream.table(
        "`claims_catalog`.`00_sql-server-staging`.`claim`"
    )


# ───────────────────────────────────────────────
# CUSTOMER (00_sql-server-staging → Bronze)
# ───────────────────────────────────────────────
@dlt.table(
    name=f"{catalog}.{bronze_schema}.customer",
    comment="Raw customer table from SQL Server staging",
    table_properties={"quality": "bronze"}
)
def customer_bronze():
    return spark.readStream.table(
        "`claims_catalog`.`00_sql-server-staging`.`customer`"
    )
