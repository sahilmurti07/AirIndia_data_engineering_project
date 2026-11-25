from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions
from pyspark import pipelines as dp

# --------------------------------------

exp_rule = {"rule_1" : "booking_id is not null",
            "rule_2" :"passenger_id is not null"
}

@dp.table(
    name = "staging_bookings"
)
def staging_bookings():
    df = spark.readStream.format('delta').load("/Volumes/airindia_flight_catalog/bronze/bronze_volume/bookings/data/")
    return df

# -----------------------------------------

@dp.table(
    name = "trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("staging_bookings")
    df = df.withColumn("booking_date",col("booking_date").cast("timestamp"))\
        .withColumn("modifiedDate",current_timestamp())\
        .withColumn("amount",col("amount").cast("double"))\
        .drop("_rescued_data")
    return df

# ----------------------------------------------

@dp.table(
    name = "silver_bookings"
)
@dp.expect_all(exp_rule)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df

# ----------------------------------------------



# airports

@dp.temporary_view()
def staging_airports():
    df = spark.readStream.format('delta').load("/Volumes/airindia_flight_catalog/bronze/bronze_volume/airports/data/")
    df = df.withColumn("modifiedDate",current_timestamp()).drop("_rescued_data")
    return df

# ----------------------------------------------

dp.create_streaming_table("silver_airports")

dp.create_auto_cdc_flow(
    target = "silver_airports",
    source = "staging_airports",
    keys = ["airport_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

# -------------------------------------------------

# passengers

@dp.temporary_view()
def staging_passengers():
    df = spark.readStream.format('delta').load("/Volumes/airindia_flight_catalog/bronze/bronze_volume/passengers/data/")
    df = df.withColumn("modifiedDate",current_timestamp()).drop("_rescued_data")
    return df


dp.create_streaming_table("silver_passengers")

dp.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "staging_passengers",
    keys = ["passenger_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

# -------------------------------------------------------
# airports

@dp.temporary_view()
def staging_flights():
    df = spark.readStream.format('delta').load("/Volumes/airindia_flight_catalog/bronze/bronze_volume/flights/data/")
    df = df.withColumn("flight_date",col("flight_date").cast("timestamp"))\
        .withColumn("modifiedDate",current_timestamp())\
        .drop("_rescued_data")
    return df

# ----------------------------------------------

dp.create_streaming_table("silver_flights")

dp.create_auto_cdc_flow(
    target = "silver_flights",
    source = "staging_flights",
    keys = ["flight_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)



# gold business view


@dp.table(
    name = "silver_obt"
)

def silver_obt():
    bookings = spark.readStream.table("silver_bookings")
    passengers = spark.read.table("silver_passengers")
    flights = spark.read.table("silver_flights")
    airports = spark.read.table("silver_airports")
    df = bookings\
        .join(passengers,["passenger_id"])\
        .join(flights,["flight_id"])\
        .join(airports,["airport_id"])\
        .drop("modifiedDate")   
    return df





