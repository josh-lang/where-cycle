from pyspark.sql.types import StructType, StructField, \
    IntegerType, TimestampType, StringType, DoubleType

green_13_16_schema = StructType(
    [
        StructField('VendorID', IntegerType(), True),
        StructField('lpep_pickup_datetime', TimestampType(), True),
        StructField('Lpep_dropoff_datetime', TimestampType(), True),
        StructField('Store_and_fwd_flag', StringType(), True),
        StructField('RateCodeID', IntegerType(), True),
        StructField('Pickup_longitude', DoubleType(), True),
        StructField('Pickup_latitude', DoubleType(), True),
        StructField('Dropoff_longitude', DoubleType(), True),
        StructField('Dropoff_latitude', DoubleType(), True)
    ]
)

yellow_09_16_schema = StructType(
    [
        StructField('VendorID', StringType(), True),
        StructField('tpep_pickup_datetime', TimestampType(), True),
        StructField('tpep_dropoff_datetime', TimestampType(), True),
        StructField('passenger_count', IntegerType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('pickup_longitude', DoubleType(), True),
        StructField('pickup_latitude', DoubleType(), True),
        StructField('RateCodeID', StringType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('dropoff_longitude', DoubleType(), True),
        StructField('dropoff_latitude', DoubleType(), True)
    ]
)

fhv_15_16_schema = StructType(
    [
        StructField('Dispatching_base_num', StringType(), True),
        StructField('Pickup_date', TimestampType(), True),
        StructField('locationID', IntegerType(), True)
    ]
)

fhv_17_19_schema = StructType(
    [
        StructField('Dispatching_base_num', StringType(), True),
        StructField('Pickup_DateTime', TimestampType(), True),
        StructField('DropOff_datetime', TimestampType(), True),
        StructField('PUlocationID', IntegerType(), True),
        StructField('DOlocationID', IntegerType(), True)
    ]
)

fhv_18_schema = StructType(
    [
        StructField('Pickup_DateTime', TimestampType(), True),
        StructField('DropOff_datetime', TimestampType(), True),
        StructField('PUlocationID', IntegerType(), True),
        StructField('DOlocationID', IntegerType(), True)
    ]
)

fhvhv_schema = StructType(
    [
        StructField('hvfhs_license_num', StringType(), True),
        StructField('dispatching_base_num', StringType(), True),
        StructField('pickup_datetime', TimestampType(), True),
        StructField('dropoff_datetime', TimestampType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True)
    ]
)

green_16_19_schema = StructType(
    [
        StructField('VendorID', IntegerType(), True),
        StructField('lpep_pickup_datetime', TimestampType(), True),
        StructField('lpep_dropoff_datetime', TimestampType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('RatecodeID', IntegerType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True)
    ]
)

yellow_16_19_schema = StructType(
    [
        StructField('VendorID', IntegerType(), True),
        StructField('tpep_pickup_datetime', TimestampType(), True),
        StructField('tpep_dropoff_datetime', TimestampType(), True),
        StructField('passenger_count', IntegerType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('RatecodeID', IntegerType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True)
    ]
)
