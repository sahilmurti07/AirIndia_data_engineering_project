from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp




@dp.temporary_view()
def silver_airports_view():
    df = spark.readStream.option("skipChangeCommits", "true").table("silver_airports")
    df = df.select(["airport_id","airport_name","city","country","modifiedDate"])
    return df

dp.create_streaming_table("dim_gold_airports")
dp.create_auto_cdc_flow(
  target = "dim_gold_airports",
  source = "silver_airports_view",
  keys = ["airport_id"],
  sequence_by = "modifiedDate",
  stored_as_scd_type = "2"
)

# --------------------------------------------------------------------------


@dp.temporary_view()
def silver_flights_view():
    df = spark.readStream.option("skipChangeCommits", "true").table("silver_flights")
    df = df.select(["flight_id","airline","origin","destination","flight_date","modifiedDate"])
    return df

dp.create_streaming_table("dim_gold_flights")
dp.create_auto_cdc_flow(
  target = "dim_gold_flights",
  source = "silver_flights_view",
  keys = ["flight_id"],
  sequence_by = "modifiedDate",
  stored_as_scd_type = "2"
)


# --------------------------------------------------------------------------


@dp.temporary_view()
def silver_passengers_view():
    df = spark.readStream.option("skipChangeCommits", "true").table("silver_passengers")
    df = df.select(["passenger_id","name","gender","nationality","modifiedDate"])
    return df

dp.create_streaming_table("dim_gold_passengers")
dp.create_auto_cdc_flow(
  target = "dim_gold_passengers",
  source = "silver_passengers_view",
  keys = ["passenger_id"],
  sequence_by = "modifiedDate",
  stored_as_scd_type = "2"
)




# -----------------------------------------------------------------------

# gold_factTable

@dp.materialized_view()
def gold_fact_table():
    df = spark.read.table("silver_obt")
    df = df.select("airport_id","flight_id","passenger_id","booking_id","amount","booking_date")
    return df




















